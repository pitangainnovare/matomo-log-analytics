# coding=utf-8
import datetime

from libs.lib_file_name import extract_file_name, INVALID_SERVERS


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

COLLECTION_TO_EXPECTED_DAILY_STATUS_SUM = {
    'arg': 1,
    'chl': 1,
    'col': 1,
    'cri': 1,
    'dat': 1,
    'ecu': 1,
    'esp': 1,
    'mex': 1,
    'nbr': {
        'after_2022_04_05': 3,
        'before_2022_04_06': 2,
    },
    'pre': 1,
    'prt': 1,
    'pry': 1,
    'psi': 1,
    'rve': 1,
    'scl': {
        'before_2021_05_25': 2,
        'after_2021_05_25': 1},
    'ssp': 2,
    'sss': 1,
    'sza': 1,
    'ury': 1,
    'ven': 1,
    'wid': 2,
}

DEFAULT_STATUS_SUM = 2


def compute_date_status(logfile_status_list, collection, date=None):
    status_sum = 0
    for s in logfile_status_list:
        if s == LOG_FILE_STATUS_LOADED:
            status_sum += 1

    if collection == 'scl':
        if date > datetime.datetime.strptime('2021-05-25', '%Y-%m-%d').date():
            expected_status_sum = COLLECTION_TO_EXPECTED_DAILY_STATUS_SUM.get(collection).get('after_2021_05_25')
        else:
            expected_status_sum = COLLECTION_TO_EXPECTED_DAILY_STATUS_SUM.get(collection).get('before_2021_05_25')
    elif collection == 'nbr':
        if date > datetime.datetime.strptime('2022-04-05', '%Y-%m-%d').date():
            expected_status_sum = COLLECTION_TO_EXPECTED_DAILY_STATUS_SUM.get(collection).get('after_2022_04_05')
        else:
            expected_status_sum = COLLECTION_TO_EXPECTED_DAILY_STATUS_SUM.get(collection).get('before_2022_04_06')
    else:
        expected_status_sum = COLLECTION_TO_EXPECTED_DAILY_STATUS_SUM.get(collection, DEFAULT_STATUS_SUM)

    if status_sum == expected_status_sum:
        return DATE_STATUS_LOADED
    elif 0 < status_sum < expected_status_sum:
        return DATE_STATUS_PARTIAL

    return DATE_STATUS_QUEUE


def is_valid_log(collection, log_file_full_path, log_file_server, log_file_date):
    date = datetime.datetime.strptime(log_file_date, '%Y-%m-%d')

    if collection == 'scl':
        # Situação em que arquivo com prefixo varnishncsa contém IPs anônimos
        if 'varnishncsa' in log_file_full_path:
            if date > datetime.datetime.strptime('2020-04-29', '%Y-%m-%d'):
                return False

        # Situação em que arquivo de servidor hiperion-apache contém IPs anônimos
        if log_file_server == 'hiperion-apache':
            if date > datetime.datetime.strptime('2020-04-29', '%Y-%m-%d'):
                return False

    if log_file_server == 'preprints':
        # Situação em que arquivo de servidor preprints contém apenas erros de acesso
        if 'error' in log_file_full_path:
            return False

        # Situação em que arquivos de outros domínios estão na mesma pasta do domínio preprints
        log_file_name = extract_file_name(log_file_full_path)
        if 'preprints' not in log_file_name:
            return False

        # Situação em que não se trata de um arquivo com extensão .log.gz
        if not log_file_name.endswith('.log.gz'):
            return False

    if collection == 'ven' and log_file_server in INVALID_SERVERS:
        return False

    return True
