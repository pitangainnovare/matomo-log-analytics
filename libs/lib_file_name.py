import datetime
import re
import os


FILE_APACHE_NAME = 'apache'
FILE_NODE03_NAME = 'node03'
FILE_HIPERION_NAME = 'hiperion'
FILE_HIPERION_APACHE_NAME = 'hiperion-apache'
FILE_HIPERION_VARNISH_NAME = 'hiperion-varnish'
FILE_NEW_BR_NAME = 'opac'
FILE_NEW_BR_NAME_1 = 'new-br1'
FILE_NEW_BR_NAME_2 = 'new-br2'
FILE_PREPRINTS_NAME = 'preprints'
FILE_DATAVERSE_NAME = 'dataverse'
FILE_DATAVERSE_HIFEN_NAME = 'data-scielo'
FILE_DATAVERSE_DOT_NAME = 'data.scielo'
FILE_DATAVERSE_NAME_1 = 'data1'
FILE_DATAVERSE_NAME_2 = 'data2'
FILE_VARNISH_NAME = 'varnish'
FILE_INFO_UNDEFINED = ''
FILE_SUMMARY_POSFIX_EXTENSION = '.summary.txt'
FILE_GUNZIPPED_LOG_EXTENSION = '.gz'
FILE_LOG_EXTENSION = '.log'
REGEX_DATE = r'\d{4}-\d{2}-\d{2}'
REGEX_DATE_NO_HYPHEN = r'[1-2]{1}\d{3}[0-1]{1}\d{1}\d{2}'


def extract_log_server_name(full_path):
    file_name = extract_file_name(full_path)

    if FILE_NODE03_NAME in full_path:
        return FILE_NODE03_NAME

    elif FILE_HIPERION_NAME in full_path:
        if FILE_APACHE_NAME in full_path:
            return FILE_HIPERION_APACHE_NAME
        if FILE_VARNISH_NAME in full_path:
            return FILE_HIPERION_VARNISH_NAME

    elif FILE_PREPRINTS_NAME in full_path:
        if FILE_PREPRINTS_NAME in file_name:
            return FILE_PREPRINTS_NAME

    elif FILE_DATAVERSE_NAME in full_path:
        if FILE_DATAVERSE_DOT_NAME in file_name:
            return FILE_DATAVERSE_NAME_2
        return FILE_DATAVERSE_NAME_1

    elif FILE_NEW_BR_NAME in full_path:
        if file_name.startswith(FILE_NEW_BR_NAME):
            return FILE_NEW_BR_NAME_2
        return FILE_NEW_BR_NAME_1

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
