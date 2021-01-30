# coding=utf-8
import re
import os


DIFF_LINES = int(os.environ.get('DIFF_LINES', '50000'))

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
    _extract_main_values(reversed(data), extracted_data)
    _extract_total_time(reversed(data), extracted_data)
    _set_status_and_lines_parsed(reversed(data), extracted_data, expected_total_lines)

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


def _set_status_and_lines_parsed(data, extracted_data, expected_lines):
    imported_lines = int(extracted_data.get('requests_imported_successfully'))
    lines_ignored = int(extracted_data.get('requests_ignored'))

    lines_parsed = imported_lines + lines_ignored

    if lines_parsed == expected_lines:
        extracted_data['status'] = 'loaded'
        extracted_data['lines_parsed'] = lines_parsed
    elif lines_parsed < expected_lines:
        extracted_data['status'] = 'partial'
        extracted_data['lines_parsed'] = lines_parsed
    else:
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
                        extracted_data['status'] = 'loaded'
                        extracted_data['lines_parsed'] = lines_parsed
                    else:
                        if lines_parsed - DIFF_LINES > 0:
                            extracted_data['lines_parsed'] = lines_parsed
                            extracted_data['status'] = 'partial'
                        else:
                            extracted_data['lines_parsed'] = 0
                            extracted_data['status'] = 'failed'
            else:
                extracted_data['lines_parsed'] = 0
                extracted_data['status'] = 'failed'
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


def read_summary(path_summary, expected_total_lines):
    with open(path_summary) as f:
        f_raw = [line.strip().lower() for line in f.readlines()]
        f_summary = extract_summary_data(f_raw, expected_total_lines)

    return f_summary
