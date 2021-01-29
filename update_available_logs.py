# coding=utf-8
import argparse
import datetime
import logging
import os
import re
import shutil

from sqlalchemy.orm.exc import NoResultFound
from libs.lib_database import get_db_session
from models.declarative import LogFile


COPY_FILES_LIMIT = int(os.environ.get('COPY_FILES_LIMIT', 6))
DIRS_USAGE_LOGS = os.environ.get('DIRS_USAGE_LOGS', '/logs_hiperion,/logs_node03').split(',')
DIR_WORKING_LOGS = os.environ.get('DIR_WORKING_LOGS', '.')
LOG_FILE_DATABASE_STRING = os.environ.get('LOG_FILE_DATABASE_STRING', 'mysql://user:pass@localhost:3306/logs_files')
LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', 'INFO')

REGEX_DATE = r'\d{4}-\d{2}-\d{2}'


def _extract_log_server_name(full_path):
    if 'node03' in full_path:
        return 'node03'
    elif 'hiperion' in full_path:
        if 'apache' in full_path:
            return 'hiperion-apache'
        if 'varnish' in full_path:
            return 'hiperion-varnish'


def _extract_log_date(full_path):
    matched_date = re.search(REGEX_DATE, full_path)
    if matched_date:
        return matched_date.group()


def _extract_name(server, date):
    file_name = server + '-' + date
    return file_name


def copy_files():
    current_files = os.listdir(DIR_WORKING_LOGS)

    db_session = get_db_session(LOG_FILE_DATABASE_STRING)

    recent_files = db_session.query(LogFile).filter(LogFile.status == 'queue').order_by(LogFile.created_at).limit(COPY_FILES_LIMIT)
    for rf in recent_files:
        rf_name_gz = rf.name + '.gz'
        if rf_name_gz not in current_files:
            source = rf.full_path
            target = os.path.join(DIR_WORKING_LOGS, rf_name_gz)

            logging.info('Copying file "%s" to "%s"' % (source, target))
            shutil.copy(source, target)

    db_session.close()


def update_log_file_table():
    db_session = get_db_session(LOG_FILE_DATABASE_STRING)

    for path in DIRS_USAGE_LOGS:
        for root, dirs, files in os.walk(path):
            for name in files:
                file = os.path.join(root, name)
                server = _extract_log_server_name(file)
                date = _extract_log_date(file)

                try:
                    existing_log_file = db_session.query(LogFile).filter(LogFile.full_path == file).one()
                except NoResultFound:
                    lf = LogFile()
                    lf.full_path = file
                    lf.created_at = datetime.datetime.fromtimestamp(os.stat(file).st_ctime)
                    lf.size = os.stat(file).st_size
                    lf.server = server
                    lf.date = date
                    lf.name = _extract_name(server, date)
                    lf.status = 'queue'

                    db_session.add(lf)
                    db_session.commit()
                    logging.info('LogFile row created: (%s, %s)' % (lf.full_path, lf.created_at.strftime('%y-%m-%d %H:%M:%S')))

    db_session.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-m', '--execution_mode',
        dest='exec_mode',
        default=['update_log_file', 'copy_logs'],
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
        logging.info('Updating log_file_table')
        update_log_file_table()
    if 'copy_logs' in params.exec_mode:
        logging.info('Copying log files to local working directory %s' % DIR_WORKING_LOGS)
        copy_files()
