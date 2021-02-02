import re


FILE_APACHE_NAME = 'apache'
FILE_NODE03_NAME = 'node03'
FILE_HIPERION_NAME = 'hiperion'
FILE_HIPERION_APACHE_NAME = 'hiperion-apache'
FILE_HIPERION_VARNISH_NAME = 'hiperion-varnish'
FILE_VARNISH_NAME = 'varnish'
FILE_INFO_UNDEFINED = ''
FILE_SUMMARY_POSFIX_EXTENSION = '.summary.txt'
FILE_GUNZIPPED_LOG_EXTENSION = '.gz'
REGEX_DATE = r'\d{4}-\d{2}-\d{2}'


def extract_log_server_name(full_path):
    if FILE_NODE03_NAME in full_path:
        return FILE_NODE03_NAME
    elif FILE_HIPERION_NAME in full_path:
        if FILE_APACHE_NAME in full_path:
            return FILE_HIPERION_APACHE_NAME
        if FILE_VARNISH_NAME in full_path:
            return FILE_HIPERION_VARNISH_NAME
    return FILE_INFO_UNDEFINED


def extract_log_date(full_path):
    matched_date = re.search(REGEX_DATE, full_path)
    if matched_date:
        return matched_date.group()
    return FILE_INFO_UNDEFINED


def extract_log_file_name(server, date):
    file_name = server + '-' + date
    return file_name


def extract_summary_file_name(file_path):
    return extract_file_name(file_path) + FILE_SUMMARY_POSFIX_EXTENSION


def extract_gunzipped_file_name(file_name):
    return file_name + FILE_GUNZIPPED_LOG_EXTENSION


def extract_file_name(file_path):
    return file_path.split('/')[-1]
