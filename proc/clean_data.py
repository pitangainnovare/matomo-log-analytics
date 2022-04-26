# coding=utf-8
import argparse
import logging
import os
import tarfile

from sys import exit
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from libs.lib_database import get_date_status_completed
from libs.lib_file_name import extract_log_date


STR_CONNECTION = os.environ.get('STR_CONNECTION', 'mysql://user:pass@localhost:3306/matomo')
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO')

ENGINE = create_engine(STR_CONNECTION)
SESSION_FACTORY = sessionmaker(bind=ENGINE)


def _get_date_file_path(directory, date_status, extension, prefix):
    if prefix:
        return os.path.join(directory, prefix + date_status.date.strftime('%Y-%m-%d') + '.' + extension)
    else:
        return os.path.join(directory, date_status.date.strftime('%Y-%m-%d') + '.' + extension)


def _compact_file(path_input, path_output):
    with tarfile.open(path_output, "w:gz") as tar:
        tar.add(path_input, arcname=os.path.basename(path_input))


def _get_date_status_completed(directory, session):
    files = os.listdir(directory)
    dates_files = [extract_log_date(f) for f in files]
    return get_date_status_completed(session, COLLECTION, dates_files)


def get_files_to_remove(directory, session, extension, prefix=None):
    date_status_completed = _get_date_status_completed(directory, session)
    return [_get_date_file_path(directory, dc, extension, prefix) for dc in date_status_completed] if date_status_completed else []


def clean_pretables(dir_zip_pretables, pretables_to_remove):
    for pt in pretables_to_remove:
        logging.info('Zipping file %s' % pt)
        head, tail = os.path.split(pt)
        path_output = os.path.join(dir_zip_pretables, tail + '.tar.gz' )
        _compact_file(path_input=pt, path_output=path_output)

        logging.info('Removing file %s' % pt)
        os.remove(pt)


def clean_r5_metrics(r5_files_to_remove):
    for r5f in r5_files_to_remove:
        logging.info('Removing file %s' % r5f)
        os.remove(r5f)


def check_dir(directory):
    if not os.path.exists(directory):
        logging.error('%s does not exist' % directory)
        exit()
    if not os.path.isdir(directory):
        logging.error('%s is not a directory' % directory)
        exit()


def main():
    usage = """Compacta e remove arquivos já processados da aplicação Matomo/COUNTER/SUSHI."""
    parser = argparse.ArgumentParser(usage)

    parser.add_argument(
        '-u', '--database_uri',
        default=LOG_FILE_DATABASE_STRING,
        dest='database_uri',
        help='String no formato mysql://username:password@host1:port/database'
    )

    parser.add_argument(
        '--dir_pretables',
        help='Diretório com arquivos de pré-tabelas',
        default=DIR_PRETABLES
    )

    parser.add_argument(
        '--dir_r5',
        help='Diretório com arquivos de métricas r5',
        default=DIR_R5_METRICS
    )

    parser.add_argument(
        '--dir_zip_pretables',
        help='Diretório com arquivos compactados de pré-tabelas',
        default=DIR_PRETABLES
    )

    parser.add_argument(
        '--logging_level',
        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET'],
        dest='logging_level',
        default='INFO'
    )

    params = parser.parse_args()
    logging.basicConfig(level=LOGGING_LEVEL,
                        format='[%(asctime)s] %(levelname)s %(message)s',
                        datefmt='%d/%b/%Y %H:%M:%S')

    for d in [
        params.dir_pretables,
        params.dir_r5,
        params.dir_zip_pretables
    ]:
        check_dir(d)

    pretables_to_remove = get_files_to_remove(params.dir_pretables, SESSION_FACTORY(), extension='tsv')
    clean_pretables(params.dir_zip_pretables, pretables_to_remove)

    r5_files_to_remove = get_files_to_remove(params.dir_r5, SESSION_FACTORY(), extension='csv', prefix='r5-metrics-')
    clean_r5_metrics(r5_files_to_remove)
