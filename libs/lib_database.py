import datetime
import logging
import os

from libs.lib_file_name import extract_log_server_name, extract_log_date, extract_log_file_name
from libs.lib_status import (
    compute_date_status,
    DATE_STATUS_PARTIAL,
    DATE_STATUS_QUEUE,
    DATE_STATUS_COMPLETED,
    LOG_FILE_STATUS_QUEUE,
    LOG_FILE_STATUS_LOADED, is_valid_log, LOG_FILE_STATUS_INVALID
)
from libs.lib_summary import parse_summary
from models.declarative import Base, DateStatus, LogFile, LogFileSummary
from sqlalchemy import create_engine, and_
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound


CRITICAL_ERROR = -999
SUCCESSFUL_RECOVERY = 999


def create_tables(database_uri):
    engine = create_engine(database_uri)
    Base.metadata.create_all(engine)


def get_recent_log_files(db_session, collection, ignore_status_list, limit=1000):
    try:
        return db_session.query(LogFile).filter(LogFile.collection == collection).filter(LogFile.status.notin_(ignore_status_list)).order_by(LogFile.date.desc()).limit(limit).all()
    except OperationalError:
        logging.info('Can\'t get recent log files. MySQL Server is unavailable.')


def get_date_status_completed(db_session, collection, dates_list):
    try:
        return db_session.query(DateStatus).filter(and_(
            DateStatus.collection == collection,
            DateStatus.status == DATE_STATUS_COMPLETED)).filter(DateStatus.date.in_(dates_list)).all()
    except OperationalError:
        logging.info('Can\'t get records from table date_status. MySQL Server is unavailable.')


def get_lines_parsed(db_session, log_file_id):
    try:
        query_result = db_session.query(LogFileSummary).filter(LogFileSummary.idlogfile == log_file_id)

        max_lines_parsed = 0

        for qr in query_result:
            if qr.lines_parsed > max_lines_parsed:
                max_lines_parsed = qr.lines_parsed

        db_session.close()

        return max_lines_parsed
    except NoResultFound:
        pass
    except OperationalError:
        logging.error('Can\'t get lines parsed. MySQL Server is unavailable.')


def update_available_log_files(db_session, dir_usage_logs, collection):
    for root, dirs, files in os.walk(dir_usage_logs):
        for name in files:
            file = os.path.join(root, name)
            server = extract_log_server_name(file, collection)
            date = extract_log_date(file)

            if server and date:
                try:
                    existing_log_file = db_session.query(LogFile).filter(and_(LogFile.full_path == file,
                                                                              LogFile.collection == collection)).one()
                except NoResultFound:
                    lf = LogFile()
                    lf.full_path = file
                    lf.created_at = datetime.datetime.fromtimestamp(os.stat(file).st_ctime)
                    lf.size = os.stat(file).st_size
                    lf.server = server
                    lf.date = date
                    lf.name = extract_log_file_name(server, date)
                    lf.status = LOG_FILE_STATUS_QUEUE
                    lf.collection = collection

                    if not is_valid_log(lf.collection, lf.full_path, lf.server, lf.date):
                        lf.status = LOG_FILE_STATUS_INVALID
                        logging.warning('LogFile row created, but will not be loaded due to unmet requirements: %s' % file)
                    else:
                        logging.info('LogFile row created: (%s, %s)' % (lf.full_path, lf.created_at.strftime('%y-%m-%d %H:%M:%S')))

                    db_session.add(lf)
                    db_session.commit()
                except OperationalError:
                    logging.error('Can\'t update available log files. MySQL Server is unavailable.')
            else:
                logging.warning('LogFile ignored due to invalid date and server: %s' % file)


def update_log_file_status(db_session, collection, file_id, status):
    try:
        log_file_row = db_session.query(LogFile).filter(and_(LogFile.collection == collection,
                                                             LogFile.id == file_id)).one()
        if log_file_row.status != status:
            if log_file_row.status != LOG_FILE_STATUS_LOADED:
                logging.info('Changing status of control_log_file.id=%s from %s to %s' % (file_id, log_file_row.status, status))
                log_file_row.status = status

        db_session.commit()
        db_session.close()
    except NoResultFound:
        pass
    except MultipleResultsFound:
        pass
    except OperationalError:
        logging.error('Can\'t update table log_file_status. MySQL Server is unavailable.')


def update_date_status(db_session, collection):
    try:
        lfdate_to_status_files = {}
        for lf in db_session.query(LogFile).filter(LogFile.collection == collection):
            if lf.date not in lfdate_to_status_files:
                lfdate_to_status_files[lf.date] = []
            lfdate_to_status_files[lf.date].append(lf.status)

        for key in lfdate_to_status_files:
            new_status = compute_date_status(lfdate_to_status_files[key], collection, date=key)

            try:
                existing_date_status = db_session.query(DateStatus).filter(and_(DateStatus.collection == collection,
                                                                                DateStatus.date == key)).one()
                if new_status != existing_date_status.status:
                    if existing_date_status.status in [DATE_STATUS_QUEUE, DATE_STATUS_PARTIAL]:
                        existing_date_status.status = new_status

            except NoResultFound:
                new_ds = DateStatus()
                new_ds.status = new_status
                new_ds.date = key
                new_ds.collection = collection

                if new_ds.date:
                    db_session.add(new_ds)

        db_session.commit()
    except NoResultFound:
        logging.error('There are no log files registered in the table log_file')
    except OperationalError:
        logging.error('Can\'t update table date_status. MySQL Server is unavailable.')


def update_log_file_summary(db_session, summary_file, expected_lines, log_file_id, recovery_directory, matomo_status):
    summary_data = parse_summary(summary_file, expected_lines)

    if matomo_status > 0:
        _save_recovery_data(recovery_directory, log_file_id, expected_lines, summary_data.get('lines_parsed', ''), summary_data.get('status', ''))
        return CRITICAL_ERROR

    try:
        lfs = LogFileSummary()
        lfs.total_lines = expected_lines
        lfs.lines_parsed = summary_data.get('lines_parsed')

        lfs.total_imported_lines = summary_data.get('requests_imported_successfully')
        lfs.total_ignored_lines = summary_data.get('requests_ignored')

        lfs.sum_imported_ignored_lines = summary_data.get('sum_imported_ignored_lines')

        lfs.ignored_lines_filtered = summary_data.get('filtered_log_lines')
        lfs.ignored_lines_http_errors = summary_data.get('http_errors')
        lfs.ignored_lines_http_redirects = summary_data.get('http_redirects')
        lfs.ignored_lines_invalid = summary_data.get('invalid_log_lines')
        lfs.ignored_lines_bots = summary_data.get('requests_done_by_bots')
        lfs.ignored_lines_static_resources = summary_data.get('requests_to_static_resources')

        lfs.total_time = summary_data.get('total_time')
        lfs.status = summary_data.get('status')

        lfs.idlogfile = log_file_id

        db_session.add(lfs)
        db_session.commit()

        return lfs.status
    except OperationalError:
        _save_recovery_data(recovery_directory, log_file_id, expected_lines, summary_data.get('lines_parsed', ''), summary_data.get('status', ''))
        return CRITICAL_ERROR


def update_log_file_summary_with_recovery_data(db_session, recovery_data):
    try:
        lfs = LogFileSummary()
        lfs.total_lines = recovery_data.get('total_lines')
        lfs.lines_parsed = recovery_data.get('lines_parsed')
        lfs.status = recovery_data.get('status')
        lfs.total_time = 0
        lfs.ignored_lines_bots = 0
        lfs.ignored_lines_filtered = 0
        lfs.ignored_lines_http_errors = 0
        lfs.ignored_lines_http_redirects = 0
        lfs.ignored_lines_invalid = 0
        lfs.ignored_lines_static_resources = 0
        lfs.sum_imported_ignored_lines = 0
        lfs.total_ignored_lines = 0
        lfs.total_imported_lines = 0

        lfs.idlogfile = recovery_data.get('idlogfile')

        db_session.add(lfs)
        db_session.commit()

        return SUCCESSFUL_RECOVERY

    except OperationalError:
        logging.error('Can\'t update log file summary with recovery data. MySQL Server is unavailable.')


def _save_recovery_data(recover_directory, log_file_id, expected_lines, parsed_lines, status):
    logging.error('Can\'t update table log_file_summary. MySQL Server is unavailable.')
    logging.warning('Creating recovery data for log file id %s.' % log_file_id)

    if not os.path.exists(recover_directory):
        os.makedirs(recover_directory)

    with open(os.path.join(recover_directory, str(log_file_id) + '.tsv'), 'a') as f:
        f.write('\t'.join([str(i) for i in [log_file_id, expected_lines, parsed_lines, status]]) + '\n')
