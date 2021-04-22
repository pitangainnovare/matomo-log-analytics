import argparse
import gzip
import re
import os


REGEX_IP = r'^(\d{1,3}\.\d{1,3})\.\d{1,3}\.\d{1,3}'


def check_file(path):
    try:
        if path.endswith('.gz'):
            gf = gzip.open(path)
            is_gzip = True
        else:
            gf = open(path)
            is_gzip = False

        ip_freq = {}
        total_lines = 0

        for l in gf:
            if is_gzip:
                decoded_line = l.decode('utf-8')
            else:
                decoded_line = l

            total_lines += 1

            matched_ip = re.search(REGEX_IP, decoded_line)
            if matched_ip:
                b2 = matched_ip.group(1)
                if b2 not in ip_freq:
                    ip_freq[b2] = 0
                ip_freq[b2] += 1

        gf.close()
        return ip_freq, total_lines

    except OSError:
        return {}


def print_ip_freq(file_name, ip_freq, total_lines, mode=1):
    if mode >= 1:
        print('File: %s, Distinct IPs: %d, Total lines: %d' % (file_name, len(ip_freq), total_lines))

    if mode >= 2:
        for i in sorted(ip_freq):
            print('File: %s, IP: %s, Freq: %d, Total lines: %d' % (file_name, i, ip_freq[i], total_lines))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', dest='dir', help='A directory that contains log files', required=True)
    parser.add_argument('-m', dest='mode', help='A mode selector (1 for default, 2 for detailed information', default=1, type=int)
    args = parser.parse_args()

    if not os.path.exists(args.dir):
        print('Directory %s does not exists' % args.dir)
        exit(1)

    for root, dirs, files in os.walk(args.dir):
        for name in files:
            file = os.path.join(root, name)
            ip_freq, total_lines = check_file(file)
            print_ip_freq(file, ip_freq, total_lines, args.mode)
