# coding=utf-8
import os
import re
import sys

ATTRS = sorted(['FILTERED_LOG_LINES',
                'HTTP_ERRORS',
                'HTTP_REDIRECTS',
                'INVALID_LOG_LINES',
                'LINES_PARSED',
                'REQUESTS_DID_NOT_MATCH_ANY_--HOSTNAME',
                'REQUESTS_DID_NOT_MATCH_ANY_KNOWN_SITE',
                'REQUESTS_DONE_BY_BOTS',
                'REQUESTS_IGNORED',
                'REQUESTS_IMPORTED_SUCCESSFULLY',
                'REQUESTS_TO_STATIC_RESOURCES',
                'REQUESTS_TO_FILE_DOWNLOADS_DID_NOT_MATCH',
                'SUCCESSFUL_LOADING',
                'TOTAL_TIME'])

HEADER = ','.join(['DATE', 'SERVER'] + ATTRS)

PATTERN_DATE = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}')
PATTERN_NUMBER = re.compile(r'[0-9]+')
PATTERN_VARNISH = re.compile(r'varnish')
PATTERN_NODE03 = re.compile(r'node03')

PATTERN_ATTR = {}

for attr in ATTRS:
    PATTERN_ATTR[attr] = re.compile(attr.replace('_', ' ').lower())


def get_log_date(log_file_name):
    match = re.search(PATTERN_DATE, log_file_name)

    if match:
        return match.group()

    print('[ERROR] Data não detectada em nome de arquivo de log %s' % log_file_name)
    exit(1)


def get_log_server(log_file_name):
    match = re.search(PATTERN_VARNISH, log_file_name)

    if match:
        return 'hiperion-varnish'

    else:
        return 'hiperion-apache'


def extract_summary_data(data, log_key):
    extracted_data = {}
    for a in ATTRS:
        extracted_data[a] = ''

    extracted_data['LINES_PARSED'] = False

    extract_others_values(reversed(data), extracted_data)
    extract_value_lines_parsed(reversed(data), extracted_data, log_key)
    extract_value_total_time(reversed(data), extracted_data)

    return extracted_data


def extract_value_total_time(data, extracted_data):
    for l in data:
        li = re.search(PATTERN_ATTR['TOTAL_TIME'], l)
        if li:
            m = re.search(PATTERN_NUMBER, l)
            if m:
                total_time = int(m.group())
                extracted_data['TOTAL_TIME'] = total_time
            break


def extract_value_lines_parsed(data, extracted_data, log_key):
    for l in data:
        if 'lines parsed' in l:
            ln = len(re.findall(PATTERN_NUMBER, l))
            if ln != 4:
                extracted_data['SUCCESSFUL_LOADING'] = False
            else:
                m = re.search(PATTERN_NUMBER, l)
                if m:
                    lines_parsed = int(m.group())
                    extracted_data['LINES_PARSED'] = lines_parsed

                    official_nlines = validation_table.get(log_key, [-1])
                    if -1 in official_nlines:
                        print('[WARNING] Não há informação oficial para (%s,%s)' % log_key)

                    if lines_parsed in official_nlines:
                        extracted_data['SUCCESSFUL_LOADING'] = True
                    else:
                        extracted_data['SUCCESSFUL_LOADING'] = False
            break


def extract_others_values(data, extracted_data):
    for l in data:
        for attr in [a for a in ATTRS if a not in {'lines parsed', 'total time'}]:

            ki_vi_line = re.search(PATTERN_ATTR[attr], l)

            if ki_vi_line:
                m = re.search(PATTERN_NUMBER, l)
                if m:
                    vi = int(m.group())
                    extracted_data[attr] = vi
                break


def read_validation_file(path_validation_table):
    date_server_to_n = {}

    with open(path_validation_table) as f:
        for i in f:
            els = i.split(' ')
            if len(els) == 2:
                date = get_log_date(els[0].strip())
                server = get_log_server(els[0].strip())
                lines = int(els[1].strip())
            else:
                print('[ERRO] Linha inválida em arquivo validador: %s' % i)
                exit(1)

            if (date, server) not in date_server_to_n:
                date_server_to_n[(date, server)] = []
            date_server_to_n[(date, server)].append(lines)

    return date_server_to_n


def read_loaded_log(path_loaded_log):
    date = get_log_date(path_loaded_log)
    server = get_log_server(path_loaded_log)

    with open(path_loaded_log) as f:
        f_raw = [line.strip().lower() for line in f.readlines()]

        log_file_name = get_log_date(path_loaded_log)
        log_file_server = get_log_server(path_loaded_log)
        log_key = (log_file_name, log_file_server)

        f_summary = extract_summary_data(f_raw, log_key)

    return (date, server), f_summary


def save_summary(date_to_summaries):
    with open('summary.csv', 'w') as f:
        f.write(HEADER + '\n')

        for date_server in sorted(date_to_summaries.keys()):
            date, server = date_server

            for summary in date_to_summaries[date_server]:
                str_values = ','.join([date, server] + [str(summary[attr]) for attr in ATTRS])
                f.write(str_values + '\n')


if __name__ == '__main__':
    validation_table = read_validation_file(sys.argv[1])

    summaries = {}

    for log_file in os.listdir(sys.argv[2]):
        date_server, summary = read_loaded_log(sys.argv[2] + '/' + log_file)
        if date_server not in summaries:
            summaries[date_server] = []
        summaries[date_server].append(summary)

    save_summary(summaries)
