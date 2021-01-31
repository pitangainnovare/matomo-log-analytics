# coding=utf-8
import logging
import os
import subprocess
import time

from libs.lib_database import (
    update_log_file_summary,
    update_log_file_status,
    update_date_status,
    get_lines_parsed,
    get_recent_log_files
)
from libs.lib_file_name import (
    extract_summary_file_name,
    extract_file_name,
    FILE_GUNZIPPED_LOG_EXTENSION,
    extract_gunzipped_file_name
)
from libs.lib_status import LOG_FILE_STATUS_LOADING


DIR_WORKING_LOGS = os.environ.get('DIR_WORKING_LOGS', '.')
DIR_SUMMARY = os.environ.get('DIR_SUMMARY', '.')
LOG_FILE_DATABASE_STRING = os.environ.get('LOG_FILE_DATABASE_STRING', 'mysql://user:pass@localhost:3306/logs_files')

LOAD_FILES_LIMIT = int(os.environ.get('LOAD_FILES_LIMIT', 4))
COLLECTION = os.environ.get('COLLECTION', 'scl')
MATOMO_ID_SITE = os.environ.get('MATOMO_ID_SITE', '1')
MATOMO_API_TOKEN = os.environ.get('MATOMO_API_TOKEN', 'e536004d5816c66e10e23a80fbd57911')
MATOMO_URL = os.environ.get('MATOMO_URL', 'http://172.17.0.4')
MATOMO_RECORDERS = os.environ.get('MATOMO_RECORDERS', '12')
RETRY_DIFF_LINES = os.environ.get('RETRY_DIFF_LINES', '100000')

LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', 'INFO')


def get_available_log_files(database_uri, collection, dir_working_logs, load_files_limit):
    files_names = set([f for f in os.listdir(dir_working_logs) if os.path.isfile(os.path.join(dir_working_logs, f))])

    db_files = get_recent_log_files(database_uri, collection, ignore_loaded=True)
    db_files_with_start_lines = set([(ef.id, ef.name, get_lines_parsed(database_uri, ef.id)) for ef in db_files])

    available_lf = set()

    file_counter = 0
    for i in db_files_with_start_lines:
        id, name, start_line = i

        gz_name = extract_gunzipped_file_name(name)
        full_path_gz_name = os.path.join(dir_working_logs, gz_name)

        alf = (id, full_path_gz_name, start_line)

        if gz_name in files_names:
            available_lf.add(alf)

            file_counter += 1
            if file_counter >= load_files_limit:
                break

    return sorted(available_lf, key=lambda x: x[1])


def generate_import_logs_params(in_file_path, out_file_path, start_line):
    matomo_attrs = {'--url': MATOMO_URL,
                    '--idsite': MATOMO_ID_SITE,
                    '--recorders': MATOMO_RECORDERS,
                    '--token-auth': MATOMO_API_TOKEN,
                    '--output': os.path.join(DIR_SUMMARY, out_file_path)}

    params = ' '.join(map('='.join, matomo_attrs.items()))

    if start_line > 0:
        params += ' --skip=' + str(start_line)

    return params + ' ' + in_file_path


def count_total_lines(log_file):
    output_line = subprocess.check_output(['wc', '-l', log_file])

    total_lines = output_line.split(' ')
    if total_lines and total_lines[0].isdigit():
        return int(total_lines[0])
    return -1


if __name__ == '__main__':
    logging.basicConfig(level=LOGGING_LEVEL,
                        format='[%(asctime)s] %(levelname)s %(message)s',
                        datefmt='%d/%b/%Y %H:%M:%S')

    if not os.path.exists(DIR_SUMMARY):
        os.makedirs(DIR_SUMMARY)

    files = get_available_log_files(LOG_FILE_DATABASE_STRING, COLLECTION, DIR_WORKING_LOGS, LOAD_FILES_LIMIT)

    for file_data in files:
        file_id, file_path, start_line = file_data
        time_start = time.time()

        logging.info('Uncompressing %s' % file_path)
        if not file_path.endswith(FILE_GUNZIPPED_LOG_EXTENSION):
            logging.error('File %s does not have a valid extension (e.g. ".gz")' % file_path)
            exit(1)
        subprocess.call('gunzip %s' % file_path, shell=True)
        gunzipped_file_path = file_path.replace(FILE_GUNZIPPED_LOG_EXTENSION, '')
        file_name = extract_file_name(gunzipped_file_path)

        summary_path_output = extract_summary_file_name(gunzipped_file_path)

        total_lines = count_total_lines(gunzipped_file_path)

        logging.info('Loading %s' % gunzipped_file_path)
        update_log_file_status(LOG_FILE_DATABASE_STRING, COLLECTION, file_name, LOG_FILE_STATUS_LOADING)
        import_logs_params = generate_import_logs_params(gunzipped_file_path, summary_path_output, start_line)
        subprocess.call('python2 libs/import_logs.py' + ' ' + import_logs_params, shell=True)

        logging.info('Updating log_file_summary with %s' % summary_path_output)
        full_path_summary_output = os.path.join(DIR_SUMMARY, summary_path_output)
        status = update_log_file_summary(LOG_FILE_DATABASE_STRING, full_path_summary_output, total_lines, file_id)

        logging.info('Removing file %s' % gunzipped_file_path)
        os.remove(gunzipped_file_path)

        logging.info('Updating log_file for row %s' % file_name)
        update_log_file_status(LOG_FILE_DATABASE_STRING, COLLECTION, file_name, status)

        logging.info('Updating date_status')
        update_date_status(LOG_FILE_DATABASE_STRING, COLLECTION)

        time_end = time.time()
        logging.info('Time spent: (%.2f) seconds' % (time_end - time_start))
