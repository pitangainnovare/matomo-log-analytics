# coding=utf-8
import argparse
import logging
import os
import shutil

from libs.lib_database import update_available_log_files, update_date_status, get_recent_log_files
from libs.lib_file_name import FILE_GUNZIPPED_LOG_EXTENSION


DIRS_USAGE_LOGS = os.environ.get('DIRS_USAGE_LOGS', '/logs_hiperion,/logs_node03').split(',')
DIR_WORKING_LOGS = os.environ.get('DIR_WORKING_LOGS', '.')
LOG_FILE_DATABASE_STRING = os.environ.get('LOG_FILE_DATABASE_STRING', 'mysql://user:pass@localhost:3306/logs_files')

COPY_FILES_LIMIT = int(os.environ.get('COPY_FILES_LIMIT', 10))
COLLECTION = os.environ.get('COLLECTION', 'scl')

LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', 'INFO')


def copy_available_log_files(database_uri, collection, dir_working_logs, copy_files_limit):
    current_files = os.listdir(dir_working_logs)

    recent_files = get_recent_log_files(database_uri, collection, ignore_loaded=True)

    for rf in sorted(recent_files.limit(copy_files_limit), key=lambda x: x.name):
        rf_name_gz = rf.name + FILE_GUNZIPPED_LOG_EXTENSION
        if rf_name_gz not in current_files:
            source = rf.full_path
            target = os.path.join(dir_working_logs, rf_name_gz)

            if os.path.exists(target):
                logging.warning('Log file %s exists' % target)
            else:
                logging.info('Copying file "%s" to "%s"' % (source, target))
                shutil.copy(source, target)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-m', '--execution_mode',
        dest='exec_mode',
        default=['update_log_file', 'copy_logs', 'date_status'],
        help='Modo de execução'
    )

    params = parser.parse_args()

    logging.basicConfig(level=LOGGING_LEVEL,
                        format='[%(asctime)s] %(levelname)s %(message)s',
                        datefmt='%d/%b/%Y %H:%M:%S')

    if not os.path.exists(DIR_WORKING_LOGS):
        logging.info('Creating DIR_WORKING_LOGS=%s' % DIR_WORKING_LOGS)
        os.makedirs(DIR_WORKING_LOGS)

    if 'update_log_file' in params.exec_mode:
        logging.info('Updating table log_file')
        update_available_log_files(LOG_FILE_DATABASE_STRING, DIRS_USAGE_LOGS, COLLECTION)

    if 'copy_logs' in params.exec_mode:
        copy_available_log_files(LOG_FILE_DATABASE_STRING, COLLECTION, DIR_WORKING_LOGS, COPY_FILES_LIMIT)

    if 'date_status' in params.exec_mode:
        logging.info('Updating table date_status')
        update_date_status(LOG_FILE_DATABASE_STRING, COLLECTION)
