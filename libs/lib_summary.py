# coding=utf-8
import re
import os

from libs.lib_status import LOG_FILE_STATUS_PARTIAL, LOG_FILE_STATUS_LOADED, LOG_FILE_STATUS_FAILED


RETRY_DIFF_LINES = int(os.environ.get('RETRY_DIFF_LINES', '100000'))

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


def _extract_summary_data(data, expected_total_lines):
    extracted_data = {}
    for a in SUMMARY_ATTRIBUTES:
        extracted_data[a] = ''

    _extract_main_values(reversed(data), extracted_data)
    _extract_total_time(reversed(data), extracted_data)
    _extract_status_and_lines_parsed(reversed(data), extracted_data, expected_total_lines)

    return extracted_data


def _extract_total_time(data, extracted_data):
    for d in data:
        li = re.search(PATTERN_ATTR['total_time'], d)
        if li:
            m = re.search(PATTERN_NUMBER, d)
            if m:
                total_time = int(m.group())
                extracted_data['total_time'] = total_time
            break


def _extract_status_and_lines_parsed(data, extracted_data, expected_lines):
    ris = extracted_data.get('requests_imported_successfully')
    li = extracted_data.get('requests_ignored')

    imported_lines = ris if ris else 0
    lines_ignored = li if li else 0

    sum_imported_ignored_lines = imported_lines + lines_ignored

    if sum_imported_ignored_lines == expected_lines:
        extracted_data['status'] = LOG_FILE_STATUS_LOADED
        extracted_data['sum_imported_ignored_lines'] = sum_imported_ignored_lines
        extracted_data['lines_parsed'] = sum_imported_ignored_lines
        return

    if 0 <= sum_imported_ignored_lines < expected_lines:
        extracted_data['sum_imported_ignored_lines'] = sum_imported_ignored_lines
    else:
        extracted_data['sum_imported_ignored_lines'] = 0

    _extract_values_failure_summary(data, extracted_data, expected_lines)


def _extract_values_failure_summary(data, extracted_data, expected_lines):
    for d in data:
        if 'lines parsed' in d:
            ln = len(re.findall(PATTERN_NUMBER, d))
            if ln == 4:
                m = re.search(PATTERN_NUMBER, d)
                if m:
                    lines_parsed = int(m.group())

                    if lines_parsed == expected_lines:
                        extracted_data['lines_parsed'] = lines_parsed
                        extracted_data['status'] = LOG_FILE_STATUS_LOADED
                    elif lines_parsed - RETRY_DIFF_LINES > 0:
                        extracted_data['lines_parsed'] = lines_parsed - RETRY_DIFF_LINES
                        extracted_data['status'] = LOG_FILE_STATUS_PARTIAL
                    else:
                        extracted_data['lines_parsed'] = 0
                        extracted_data['status'] = LOG_FILE_STATUS_FAILED
            else:
                extracted_data['lines_parsed'] = 0
                extracted_data['status'] = LOG_FILE_STATUS_FAILED
            break


def _extract_main_values(data, extracted_data):
    for d in data:
        for d_attr in [a for a in SUMMARY_ATTRIBUTES if a not in {'lines_parsed', 'total_time'}]:

            ki_vi_line = re.search(PATTERN_ATTR[d_attr], d)

            if ki_vi_line:
                m = re.search(PATTERN_NUMBER, d)
                if m:
                    vi = int(m.group())
                    extracted_data[d_attr] = vi
                break


def parse_summary(path_summary, expected_total_lines):
    with open(path_summary) as f:
        f_raw = [line.strip().lower() for line in f.readlines()]
        f_summary = _extract_summary_data(f_raw, expected_total_lines)

    return f_summary
