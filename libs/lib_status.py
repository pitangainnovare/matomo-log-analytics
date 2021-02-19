# coding=utf-8
import datetime

DATE_STATUS_QUEUE = 0
DATE_STATUS_PARTIAL = 1
DATE_STATUS_LOADED = 2
DATE_STATUS_PRETABLE = 3
DATE_STATUS_COMPUTED = 4
DATE_STATUS_COMPLETED = 5

LOG_FILE_STATUS_QUEUE = 0
LOG_FILE_STATUS_PARTIAL = 1
LOG_FILE_STATUS_LOADED = 2
LOG_FILE_STATUS_LOADING = 9
LOG_FILE_STATUS_FAILED = -1
LOG_FILE_STATUS_INVALID = -9

DATE_STATUS_SUM_FOR_LOADED = 2


def compute_date_status(logfile_status_list):
    status_sum = 0
    for s in logfile_status_list:
        if s == LOG_FILE_STATUS_LOADED:
            status_sum += 1

    if status_sum == DATE_STATUS_SUM_FOR_LOADED:
        return DATE_STATUS_LOADED
    elif 0 < status_sum < DATE_STATUS_SUM_FOR_LOADED:
        return DATE_STATUS_PARTIAL

    return DATE_STATUS_QUEUE


def is_valid_log(log_file_full_path, log_file_server, log_file_date):
    date = datetime.datetime.strptime(log_file_date, '%Y-%m-%d')

    # Situação em que arquivo com prefixo varnishncsa contém IPs anônimos
    if 'varnishncsa' in log_file_full_path:
        if date > datetime.datetime.strptime('2020-04-29', '%Y-%m-%d'):
            return False

    # Situação em que arquivo de servidor hiperion-apache contém IPs anônimos
    if log_file_server == 'hiperion-apache':
        if date > datetime.datetime.strptime('2020-04-29', '%Y-%m-%d'):
            return False

    # Situação em que arquivo de servidor preprints contém apenas erros de acesso
    if log_file_server == 'preprints':
        if 'error' in log_file_full_path:
            return False

    return True
