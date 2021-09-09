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


