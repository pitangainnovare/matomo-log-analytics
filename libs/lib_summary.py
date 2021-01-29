# coding=utf-8
import re


SUMMARY_ATTRIBUTES = sorted(['filtered_log_lines',
                             'http_errors',
                             'http_redirects',
                             'invalid_log_lines',
                             'lines_parsed',
                             'requests_done_by_bots',
                             'requests_ignored',
                             'requests_imported_successfully',
                             'requests_to_static_resources',
                             'status',
                             'total_time'])

PATTERN_DATE = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}')
PATTERN_NUMBER = re.compile(r'[0-9]+')
PATTERN_VARNISH = re.compile(r'varnish')
PATTERN_NODE03 = re.compile(r'node03')

PATTERN_ATTR = {}

for attr in SUMMARY_ATTRIBUTES:
    PATTERN_ATTR[attr] = re.compile(attr.replace('_', ' ').lower())


def extract_summary_data(data, expected_total_lines):
    extracted_data = {}
    for a in SUMMARY_ATTRIBUTES:
        extracted_data[a] = ''

    extracted_data['status'] = ''
    extract_main_values(reversed(data), extracted_data)
    extract_time_spent(reversed(data), extracted_data)
    set_status(extracted_data, expected_total_lines)

    return extracted_data


def extract_time_spent(data, extracted_data):
    for d in data:
        li = re.search(PATTERN_ATTR['total_time'], d)
        if li:
            m = re.search(PATTERN_NUMBER, d)
            if m:
                total_time = int(m.group())
                extracted_data['total_time'] = total_time
            break


def set_status(extracted_data, expected_lines):
    imported_lines = int(extracted_data.get('requests_imported_successfully', '-1'))
    lines_ignored = int(extracted_data.get('requests_ignored', '-1'))

    lines_parsed = imported_lines + lines_ignored
    if lines_parsed == expected_lines:
        extracted_data['status'] = 'completed'
    elif lines_parsed < expected_lines:
        extracted_data['status'] = 'partial'
    else:
        extracted_data['status'] = 'failed'

    extracted_data['lines_parsed'] = lines_parsed


def extract_main_values(data, extracted_data):
    for d in data:
        for d_attr in [a for a in SUMMARY_ATTRIBUTES if a not in {'lines_parsed', 'total_time'}]:

            ki_vi_line = re.search(PATTERN_ATTR[d_attr], d)

            if ki_vi_line:
                m = re.search(PATTERN_NUMBER, d)
                if m:
                    vi = int(m.group())
                    extracted_data[d_attr] = vi
                break


def read_summary(path_summary, expected_total_lines):
    with open(path_summary) as f:
        f_raw = [line.strip().lower() for line in f.readlines()]
        f_summary = extract_summary_data(f_raw, expected_total_lines)

    return f_summary
