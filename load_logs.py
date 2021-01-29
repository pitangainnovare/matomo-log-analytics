# coding=utf-8
import logging
import os
import re
import subprocess
import time

from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from libs.lib_database import get_db_session
from libs.lib_summary import read_summary
from models.declarative import LogFile, LogFileSummary
from update_available_logs import update_date_status_table

DIR_WORKING_LOGS = os.environ.get('DIR_WORKING_LOGS', '.')
DIR_WORKING_LOADED_LOGS = os.environ.get('DIR_WORKING_LOADED_LOGS', '.')
LOG_FILE_DATABASE_STRING = os.environ.get('LOG_FILE_DATABASE_STRING', 'mysql://user:pass@localhost:3306/logs_files')
LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', 'INFO')
COLLECTION = os.environ.get('COLLECTION', 'scl')
MATOMO_ID_SITE = os.environ.get('MATOMO_ID_SITE', '1')
MATOMO_API_TOKEN = os.environ.get('MATOMO_API_TOKEN', 'e536004d5816c66e10e23a80fbd57911')
MATOMO_URL = os.environ.get('MATOMO_URL', 'http://172.17.0.4')
MATOMO_RECORDERS = os.environ.get('MATOMO_RECORDERS', '12')


def get_files():
    files_names = set(os.listdir(DIR_WORKING_LOGS))

    db_session = get_db_session(LOG_FILE_DATABASE_STRING)
    try:
        existing_files = db_session.query(LogFile).filter(LogFile.collection == COLLECTION).filter(LogFile.name.in_(files_names))
    except NoResultFound:
        logging.debug('Não há informação de arquivos na tabela LogFile')
        existing_files = set()

    efs = set([ef.name for ef in existing_files if ef.status == 'completed'])
    return sorted([os.path.join(DIR_WORKING_LOGS, f) for f in files_names.difference(efs)])


def _generate_summary_file_name(file_path):
    return _extract_file_name(file_path) + '-loaded.txt'


def _generate_import_logs_params(in_file_path, out_file_path):
    matomo_attrs = {'--url': MATOMO_URL,
                    '--idsite': MATOMO_ID_SITE,
                    '--recorders': MATOMO_RECORDERS,
                    '--token-auth': MATOMO_API_TOKEN,
                    '--output': os.path.join(DIR_WORKING_LOADED_LOGS, out_file_path)}

    return ' '.join(map('='.join, matomo_attrs.items())) + ' ' + in_file_path


def _extract_file_name(file_path):
    return file_path.split('/')[-1]


def _count_total_lines(file):
    output_line = subprocess.check_output(['wc', '-l', file])

    total_lines = output_line.split(' ')
    if total_lines and total_lines[0].isdigit():
        return int(total_lines[0])
    return -1


def update_log_file_summary_table(summary_file, expected_lines, file_name):
    summary_data = read_summary(summary_file, expected_lines)

    db_session = get_db_session(LOG_FILE_DATABASE_STRING)

    lfs = LogFileSummary()
    lfs.total_lines = expected_lines
    lfs.lines_parsed = summary_data.get('lines_parsed')

    lfs.total_imported_lines = summary_data.get('requests_imported_successfully')
    lfs.total_ignored_lines = summary_data.get('requests_ignored')

    lfs.ignored_lines_filtered = summary_data.get('filtered_log_lines')
    lfs.ignored_lines_http_errors = summary_data.get('http_errors')
    lfs.ignored_lines_http_redirects = summary_data.get('http_redirects')
    lfs.ignored_lines_invalid = summary_data.get('invalid_log_lines')
    lfs.ignored_lines_bots = summary_data.get('requests_done_by_bots')
    lfs.ignored_lines_static_resources = summary_data.get('requests_to_static_resources')

    lfs.total_time = summary_data.get('total_time')
    lfs.status = summary_data.get('status')

    try:
        idlogfile = db_session.query(LogFile).filter(LogFile.name == file_name).one().id
    except NoResultFound:
        idlogfile = -1

    lfs.idlogfile = idlogfile

    db_session.add(lfs)
    db_session.commit()

    return lfs.status


def update_log_file_table(file_name, status):
    db_session = get_db_session(LOG_FILE_DATABASE_STRING)

    try:
        log_file_row = db_session.query(LogFile).filter(LogFile.collection == COLLECTION).filter(LogFile.name == file_name).one()
        if log_file_row.status != status:
            if log_file_row.status != 'completed':
                logging.info('Changing status of %s from %s to %s' % (file_name, log_file_row.status, status))
                log_file_row.status = status
    except NoResultFound:
        pass
    except MultipleResultsFound:
        pass

    db_session.commit()


if __name__ == '__main__':
    logging.basicConfig(level=LOGGING_LEVEL,
                        format='[%(asctime)s] %(levelname)s %(message)s',
                        datefmt='%d/%b/%Y %H:%M:%S')

    if not os.path.exists(DIR_WORKING_LOADED_LOGS):
        os.makedirs(DIR_WORKING_LOADED_LOGS)

    files = get_files()

    for file_path in files:
        time_start = time.time()

        logging.info('Uncompressing %s' % file_path)
        if not file_path.endswith('.gz'):
            logging.error('File %s does not have a valid extension (e.g. ".gz")')
            exit(1)
        subprocess.call('gunzip %s' % file_path, shell=True)
        gunzipped_file_path = file_path.replace('.gz', '')
        file_name = _extract_file_name(gunzipped_file_path)

        summary_path_output = _generate_summary_file_name(gunzipped_file_path)

        total_lines = _count_total_lines(gunzipped_file_path)

        logging.info('Loading %s' % gunzipped_file_path)
        import_logs_params = _generate_import_logs_params(gunzipped_file_path, summary_path_output)
        subprocess.call('python2 libs/import_logs.py' + ' ' + import_logs_params, shell=True)

        logging.info('Updating log_file_summary with %s' % summary_path_output)
        full_path_summary_output = os.path.join(DIR_WORKING_LOADED_LOGS, summary_path_output)
        status = update_log_file_summary_table(full_path_summary_output, expected_lines=total_lines, file_name=file_name)

        if status == 'completed':
            logging.info('Removing file %s' % gunzipped_file_path)
            os.remove(gunzipped_file_path)

        logging.info('Updating log_file for row %s' % file_name)
        update_log_file_table(file_name, status)

        logging.info('Updating date_status')
        update_date_status_table()

        time_end = time.time()
        logging.info('Time spent: (%.2f) seconds' % (time_end - time_start))
