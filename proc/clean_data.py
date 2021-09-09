# coding=utf-8
import argparse
import logging
import magic
import os
import subprocess
import tarfile
import time

from sys import exit
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from libs.lib_database import get_date_status_completed
from libs.lib_file_name import extract_log_date
from libs.lib_status import LOG_FILE_STATUS_LOADING, LOG_FILE_STATUS_INVALID, LOG_FILE_STATUS_LOADED


COLLECTION = os.environ.get('COLLECTION', 'scl')
DIR_PRETABLES = os.environ.get('DIR_PRETABLES', os.path.join('/app/data/pretables', COLLECTION))
DIR_ZIPS_PRETABLES = os.environ.get('DIR_ZIPS_PRETABLES', os.path.join('/app/data/zips/pretables', COLLECTION))
DIR_R5_METRICS = os.environ.get('DIR_R5_METRICS', os.path.join('/app/data/r5', COLLECTION))

LOG_FILE_DATABASE_STRING = os.environ.get('LOG_FILE_DATABASE_STRING', 'mysql://user:pass@localhost:3306/matomo')

LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', 'INFO')

ENGINE = create_engine(LOG_FILE_DATABASE_STRING)
SESSION_FACTORY = sessionmaker(bind=ENGINE)


def _get_date_file_path(directory, date_status, extension, prefix):
    if prefix:
        return os.path.join(directory, prefix + date_status.date.strftime('%Y-%m-%d') + '.' + extension)
    else:
        return os.path.join(directory, date_status.date.strftime('%Y-%m-%d') + '.' + extension)


def _compact_file(path_input, path_output):
    with tarfile.open(path_output, "w:gz") as tar:
        tar.add(path_input, arcname=os.path.basename(path_input))
