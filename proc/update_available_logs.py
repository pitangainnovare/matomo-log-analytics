# coding=utf-8
import argparse
import logging
import os
import shutil

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from libs.lib_file_name import add_gunzip_extension
from libs.lib_status import LOG_FILE_STATUS_INVALID, LOG_FILE_STATUS_LOADED
from sqlalchemy.exc import OperationalError
from libs.lib_database import (
    update_available_log_files,
    update_date_status,
    get_recent_log_files,
    update_log_file_status,
    update_log_file_summary_with_recovery_data
)


DIR_USAGE_LOGS_1 = os.environ.get('DIR_USAGE_LOGS_1', '/app/usage-logs-1')
DIR_USAGE_LOGS_2 = os.environ.get('DIR_USAGE_LOGS_2', '/app/usage-logs-2')
DIR_WORKING_LOGS = os.environ.get('DIR_WORKING_LOGS', '/app/data/working')
DIR_SUMMARY = os.environ.get('DIR_SUMMARY', '/app/data/summary')
DIR_RECOVERY = os.path.join(DIR_SUMMARY, 'recovery')
LOG_FILE_DATABASE_STRING = os.environ.get('LOG_FILE_DATABASE_STRING', 'mysql://user:pass@localhost:3306/matomo')

COPY_FILES_LIMIT = int(os.environ.get('COPY_FILES_LIMIT', 10))
COLLECTION = os.environ.get('COLLECTION', 'scl')
RETRY_DIFF_LINES = int(os.environ.get('RETRY_DIFF_LINES', '110000'))

LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', 'INFO')

ENGINE = create_engine(LOG_FILE_DATABASE_STRING)
SESSION_FACTORY = sessionmaker(bind=ENGINE)

def copy_available_log_files(database_uri, collection, dir_working_logs, copy_files_limit):
    current_files = os.listdir(dir_working_logs)

    recent_files = get_recent_log_files(database_uri,
                                        collection,
                                        ignore_status_list=[LOG_FILE_STATUS_LOADED, LOG_FILE_STATUS_INVALID])

    for rf in recent_files.limit(copy_files_limit):
        rf_name_gz = add_gunzip_extension(rf.name)
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
        for dir_log in [DIR_USAGE_LOGS_1, DIR_USAGE_LOGS_2]:
            logging.info('Updating table log_file with possible new logs in %s' % dir_log)
            update_available_log_files(LOG_FILE_DATABASE_STRING, dir_log, COLLECTION)

    if 'copy_logs' in params.exec_mode:
        copy_available_log_files(LOG_FILE_DATABASE_STRING, COLLECTION, DIR_WORKING_LOGS, COPY_FILES_LIMIT)

    if 'date_status' in params.exec_mode:
        logging.info('Updating table date_status')
        update_date_status(LOG_FILE_DATABASE_STRING, COLLECTION)
