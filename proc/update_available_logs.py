# coding=utf-8
import argparse
import logging
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from libs import lib_database


COLLECTION = os.environ.get('COLLECTION', 'scl')
DIR_USAGE_LOGS = os.environ.get('DIR_USAGE_LOGS', '/app/usage-logs')
LOG_FILE_DATABASE_STRING = os.environ.get('LOG_FILE_DATABASE_STRING', 'mysql://user:pass@localhost:3306/matomo')
LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', 'INFO')

ENGINE = create_engine(LOG_FILE_DATABASE_STRING)
SESSION_FACTORY = sessionmaker(bind=ENGINE)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--execution_mode',
        action='append',
        help='Modos de execução',
        choices=[
            'update_table_log_file',
            'update_table_date_status',
        ]
    )

    parser.add_argument(
        '--dir_usage_logs',
        help='Diretório com arquivos de log',
        default=DIR_USAGE_LOGS,
    )

    params = parser.parse_args()

    logging.basicConfig(level=LOGGING_LEVEL,
                        format='[%(asctime)s] %(levelname)s %(message)s',
                        datefmt='%d/%b/%Y %H:%M:%S')

    if 'update_table_log_file' in params.execution_mode:
        logging.info('Updating table log_file with possible new logs in %s' % params.dir_usage_logs)
        lib_database.update_available_log_files(SESSION_FACTORY(), params.dir_usage_logs, COLLECTION)

    if 'update_table_date_status' in params.execution_mode:
        logging.info('Updating table date_status')
        lib_database.update_date_status(SESSION_FACTORY(), COLLECTION)
