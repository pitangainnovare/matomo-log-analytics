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


def copy_available_log_files(db_session, collection, dir_working_logs, copy_files_limit):
    current_files = os.listdir(dir_working_logs)

    recent_files = get_recent_log_files(db_session,
                                        collection,
                                        ignore_status_list=[LOG_FILE_STATUS_LOADED, LOG_FILE_STATUS_INVALID],
                                        limit=copy_files_limit)

    if recent_files:
        try:
            for rf in recent_files:
                rf_name_gz = add_gunzip_extension(rf.name)
                if rf_name_gz not in current_files:
                    source = rf.full_path
                    target = os.path.join(dir_working_logs, rf_name_gz)

                    if os.path.exists(target):
                        logging.warning('Log file %s exists' % target)
                    else:
                        logging.info('Copying file "%s" to "%s"' % (source, target))
                        shutil.copy(source, target)
        except OperationalError or TypeError:
            logging.error('Can\'t copy available log files. MySQL Server is unavailable.')


def check_and_fix_recovery_data():
    for rf in os.listdir(DIR_RECOVERY):
        full_rf_path = os.path.join(DIR_RECOVERY, rf)

        with open(full_rf_path) as f:
            lines = f.readlines()

            for row in reversed(lines):
                if row:
                    els = row.strip().split('\t')

                    idlogfile = int(els[0])
                    total_lines = int(els[1])
                    lines_parsed = int(els[2])
                    status = int(els[3])

                    break

            if status == 0 and lines_parsed == 0:
                logging.info('Removing file %s...' % full_rf_path)
                os.remove(full_rf_path)

            else:
                adjusted_lines_parsed = lines_parsed - RETRY_DIFF_LINES

                if adjusted_lines_parsed > 0:
                    rf_data = {'status': status,
                               'lines_parsed': adjusted_lines_parsed,
                               'total_lines': total_lines,
                               'idlogfile': idlogfile}

                    logging.info('Updating table control_log_file_summary...')
                    success = update_log_file_summary_with_recovery_data(SESSION_FACTORY(), rf_data)

                    if success:
                        logging.info('Recovered from critical error. File %s was added to log_file_summary' % full_rf_path)
                        logging.info('Updating table control_log_file')
                        update_log_file_status(SESSION_FACTORY(),
                                               COLLECTION,
                                               rf_data.get('idlogfile'),
                                               rf_data.get('status'))

                        logging.info('Updating table control_date_status...')
                        update_date_status(SESSION_FACTORY(), COLLECTION)

                        logging.info('Removing file %s...' % full_rf_path)
                        os.remove(full_rf_path)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--execution_mode',
        action='append',
        help='Modos de execução',
        choices=[
            'update_table_log_file',
            'update_table_date_status',
            'copy_logs']
    )

    params = parser.parse_args()

    logging.basicConfig(level=LOGGING_LEVEL,
                        format='[%(asctime)s] %(levelname)s %(message)s',
                        datefmt='%d/%b/%Y %H:%M:%S')

    if not os.path.exists(DIR_WORKING_LOGS):
        logging.info('Creating DIR_WORKING_LOGS=%s' % DIR_WORKING_LOGS)
        os.makedirs(DIR_WORKING_LOGS)

    if not os.path.exists(DIR_RECOVERY):
        logging.info('Creating DIR_RECOVERY=%s' % DIR_RECOVERY)
        os.makedirs(DIR_RECOVERY)

    logging.info('Checking for recovery data...')
    check_and_fix_recovery_data()

    if 'update_table_log_file' in params.execution_mode:
        for dir_log in [DIR_USAGE_LOGS_1, DIR_USAGE_LOGS_2]:
            logging.info('Updating table log_file with possible new logs in %s' % dir_log)
            update_available_log_files(SESSION_FACTORY(), dir_log, COLLECTION)

    if 'copy_logs' in params.execution_mode:
        copy_available_log_files(SESSION_FACTORY(), COLLECTION, DIR_WORKING_LOGS, COPY_FILES_LIMIT)

    if 'update_table_date_status' in params.execution_mode:
        logging.info('Updating table date_status')
        update_date_status(SESSION_FACTORY(), COLLECTION)
