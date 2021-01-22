# coding=utf-8
import argparse
import os
import re


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
                'OFFICIAL_SERVER',
                'OFFICIAL_NLINES',
                'TOTAL_TIME'])

HEADER = ','.join(['DATE', 'SERVER', 'URL'] + ATTRS)

PATTERN_DATE = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}')
PATTERN_NUMBER = re.compile(r'[0-9]+')
PATTERN_VARNISH = re.compile(r'varnish')
PATTERN_NODE03 = re.compile(r'node03')
PATTERN_URL = re.compile(r'--url=\'(.*)\'')

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
    match_is_varnish = re.search(PATTERN_VARNISH, log_file_name)

    if match_is_varnish:
        return 'hiperion-varnish'
    else:
        match_is_node03 = re.search(PATTERN_NODE03, log_file_name)

        if match_is_node03:
            return 'hiperion-node03'

    return 'hiperion-apache'


def extract_summary_data(data, log_key):
    extracted_data = {}
    for a in ATTRS:
        extracted_data[a] = ''

    extracted_data['SUCCESSFUL_LOADING'] = False
    extracted_data['OFFICIAL_NLINES'] = -1
    extracted_data['OFFICIAL_SERVER'] = -1

    extract_others_values(reversed(data), extracted_data)
    extract_value_lines_parsed(reversed(data), extracted_data, log_key)
    extract_value_total_time(reversed(data), extracted_data)
    extract_value_url(reversed(data), extracted_data)

    return extracted_data


def extract_value_total_time(data, extracted_data):
    for d in data:
        li = re.search(PATTERN_ATTR['TOTAL_TIME'], d)
        if li:
            m = re.search(PATTERN_NUMBER, d)
            if m:
                total_time = int(m.group())
                extracted_data['TOTAL_TIME'] = total_time
            break


def extract_value_lines_parsed(data, extracted_data, log_key):
    for d in data:
        if 'lines parsed' in d:
            ln = len(re.findall(PATTERN_NUMBER, d))
            if ln != 4:
                extracted_data['SUCCESSFUL_LOADING'] = False
            else:
                m = re.search(PATTERN_NUMBER, d)
                if m:
                    lines_parsed = int(m.group())
                    extracted_data['LINES_PARSED'] = lines_parsed

                    date, server = log_key
                    official_nlines = validation_date_to_lines_list.get(date, [-1])
                    if -1 in official_nlines:
                        print('[WARNING] Não há informação oficial para (%s,%s)' % log_key)

                    for ol in official_nlines:
                        if lines_parsed == ol:
                            extracted_data['SUCCESSFUL_LOADING'] = True
                            extracted_data['OFFICIAL_NLINES'] = ol

                            official_server = validation_date_server_to_line.get((date, ol), -1)
                            extracted_data['OFFICIAL_SERVER'] = official_server

                            break
            break


def extract_others_values(data, extracted_data):
    for d in data:
        for d_attr in [a for a in ATTRS if a not in {'lines parsed', 'total time'}]:

            ki_vi_line = re.search(PATTERN_ATTR[d_attr], d)

            if ki_vi_line:
                m = re.search(PATTERN_NUMBER, d)
                if m:
                    vi = int(m.group())
                    extracted_data[d_attr] = vi
                break


def extract_value_url(data, extracted_data):
    for d in data:
        m = re.search(PATTERN_URL, d)
        if m:
            extracted_data['URL'] = m.group(1)
            break

    if 'URL' not in extracted_data:
        extracted_data['URL'] = 'https://matomo.scielo.org'


def read_validation_file(path_validation_table):
    date_to_lines_list = {}
    date_line_to_server = {}

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

            if date not in date_to_lines_list:
                date_to_lines_list[date] = []
            date_to_lines_list[date].append(lines)

            if (date, lines) not in date_line_to_server:
                date_line_to_server[(date, lines)] = server
            else:
                print('[ERRO] (%s, %s) já existe' % (date, lines))

    return date_to_lines_list, date_line_to_server


def read_loaded_log(path_loaded_log):
    date = get_log_date(path_loaded_log)
    server = get_log_server(path_loaded_log)

    with open(path_loaded_log) as f:
        f_raw = [line.strip().lower() for line in f.readlines()]

        log_date = get_log_date(path_loaded_log)
        log_server = get_log_server(path_loaded_log)
        log_key = (log_date, log_server)

        f_summary = extract_summary_data(f_raw, log_key)

    return (date, server), f_summary


def save_summary(date_to_summaries):
    with open('summary.csv', 'w') as f:
        f.write(HEADER + '\n')

        for key_date_server in sorted(date_to_summaries.keys()):
            date, server = key_date_server

            for d_summary in date_to_summaries[key_date_server]:
                str_values = ','.join([date, server, d_summary['URL']] + [str(d_summary[d_attr]) for d_attr in ATTRS])
                f.write(str_values + '\n')


if __name__ == '__main__':
    usage = 'Extrai arquivo de sumário de logs carregados no Matomo'
    parser = argparse.ArgumentParser(usage)

    parser.add_argument(
        '-v', '--validation_file',
        dest='validation_file',
        required=True,
        help='Arquivo contendo lista de caminhos de logs de acessos e respectivos números de linhas'
    )

    parser.add_argument(
        '-l', '--logs_dir',
        dest='logs_dir',
        required=True,
        help='Pasta com arquivos de sumário de carga de logs no Matomo'
    )

    params = parser.parse_args()

    validation_date_to_lines_list, validation_date_server_to_line = read_validation_file(params.validation_file)

    summaries = {}

    for log_file in os.listdir(params.logs_dir):
        date_server, summary = read_loaded_log(os.path.join(params.logs_dir, log_file))
        if date_server not in summaries:
            summaries[date_server] = []
        summaries[date_server].append(summary)

    save_summary(summaries)
