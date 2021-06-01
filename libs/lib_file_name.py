# coding=utf-8
import datetime
import logging
import re
import os

from libs.values import *


COLLECTION = os.environ.get('COLLECTION', 'scl')
LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', 'INFO')

FILE_SUMMARY_POSFIX_EXTENSION = '.summary.txt'
FILE_GUNZIPPED_LOG_EXTENSION = '.gz'
FILE_LOG_EXTENSION = '.log'
REGEX_DATE = r'\d{4}-\d{2}-\d{2}'
REGEX_DATE_NO_HYPHEN = r'[1-2]{1}\d{3}[0-1]{1}\d{1}\d{2}'

logging.basicConfig(level=LOGGING_LEVEL,
                    format='[%(asctime)s] %(levelname)s %(message)s',
                    datefmt='%d/%b/%Y %H:%M:%S')


def _check_brasil(full_path, file_name):
    if FILE_NODE03_NAME in full_path:
        return FILE_NODE03_NAME

    elif FILE_HIPERION_NAME in full_path:
        if FILE_APACHE_NAME in full_path:
            return FILE_HIPERION_APACHE_NAME

        if FILE_VARNISH_NAME in full_path:
            return FILE_HIPERION_VARNISH_NAME


def _check_dataverse(full_path, file_name):
    if FILE_DATAVERSE_NAME in full_path:
        if FILE_DATAVERSE_DOT_NAME in file_name:
            return FILE_DATAVERSE_NAME_2

        return FILE_DATAVERSE_NAME_1


def _check_preprints(full_path, file_name):
    if FILE_PREPRINTS_NAME in full_path:
        if FILE_PREPRINTS_NAME in file_name:
            return FILE_PREPRINTS_NAME


def _check_ratchet(full_path, file_name):
    results = []

    for k in PARTIAL_FILE_NAME_TO_SERVER:
        if k in file_name:
            server_prefix, server_number = PARTIAL_FILE_NAME_TO_SERVER[k]

            if COLLECTION in server_prefix:
                results.append(''.join(PARTIAL_FILE_NAME_TO_SERVER[k]))

    if len(results) == 1:
        return results[0]

    elif len(results) > 1:
        logging.error('%s pertence a mais de uma coleção.' % full_path)


def _check_new_brasil(full_path, file_name):
    if FILE_NEW_BR_VARNISH02_NAME in full_path:
        return FILE_NEW_BR_NAME_3
    elif FILE_NEW_BR_VARNISH03_NAME in full_path:
        return FILE_NEW_BR_NAME_4


def _check_venezuela(full_path, file_name):
    if FILE_VENEZUELA_APACHE_NAME in full_path:
        if FILE_VENEZUELA_CENTOS01_NAME in full_path:
            if re.search(REGEX_VENEZUELA_STARTS_WITH_DATE, file_name):
                return FILE_VENEZUELA_NAME_1

            elif re.search(REGEX_VENEZUELA_ENDS_WITH_DATE, file_name):
                return FILE_VENEZUELA_NAME_3

            elif re.search(REGEX_VENEZUELA_ENDS_WITH_DATE_NO_HIPHEN, file_name):
                return FILE_VENEZUELA_NAME_4

        elif FILE_VENEZUELA_CENTOS02_NAME in full_path:
            if FILE_VENEZUELA_CENTOS02_ORG_VE_NAME in full_path:
                if re.search(REGEX_VENEZUELA_ENDS_WITH_DATE, file_name):
                    return FILE_VENEZUELA_NAME_5

            elif FILE_VENEZUELA_CENTOS02_VARNISH_NAME in full_path:
                return FILE_VENEZUELA_NAME_6

        elif FILE_VENEZUELA_GENERIC_NAME_1 in file_name:
            if re.search(REGEX_VENEZUELA_STARTS_WITH_DATE, file_name):
                return FILE_VENEZUELA_NAME_1

        elif FILE_VENEZUELA_GENERIC_NAME_2 in file_name:
            if re.search(REGEX_VENEZUELA_STARTS_WITH_DATE, file_name):
                return FILE_VENEZUELA_NAME_2

    elif FILE_VENEZUELA_HA_NAME in full_path:
        return FILE_VENEZUELA_NAME_7


def extract_log_server_name(full_path):
    collection_to_check_method = {
        'nbr': _check_new_brasil,
        'scl': _check_brasil,
        'ven': _check_venezuela,
        'dat': _check_dataverse,
        'pre': _check_preprints
    }

    file_name = extract_file_name(full_path)
    check_method = collection_to_check_method.get(COLLECTION, _check_ratchet)
    check_result = check_method(full_path, file_name)

    if check_result:
        return check_result
    else:
        return FILE_INFO_UNDEFINED


def _detect_date_full_path(regex, full_path):
    match = re.search(regex, full_path)
    if match:
        return match.group()
    return ''


def _try_create_date_from_str(date_str, fmt):
    try:
        return datetime.datetime.strptime(date_str, fmt)
    except ValueError:
        return ''


def extract_log_date(full_path):
    for regex_format in [(REGEX_DATE, '%Y-%m-%d'), (REGEX_DATE_NO_HYPHEN, '%Y%m%d')]:
        regex, format = regex_format

        date_str = _detect_date_full_path(regex, full_path)

        valid_date = _try_create_date_from_str(date_str, format)
        if valid_date:
            return valid_date.strftime('%Y-%m-%d')

    return FILE_INFO_UNDEFINED


def extract_log_file_name(server, date):
    file_name = server + '-' + date
    return file_name


def add_summary_extension(file_path):
    return extract_file_name(file_path) + FILE_SUMMARY_POSFIX_EXTENSION


def add_gunzip_extension(file_name):
    return file_name + FILE_GUNZIPPED_LOG_EXTENSION


def extract_file_name(file_path):
    head_tail = os.path.split(file_path)
    file_name = head_tail[1]
    return file_name
