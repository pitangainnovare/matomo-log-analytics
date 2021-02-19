import datetime
import logging
import os

from libs.lib_file_name import extract_log_server_name, extract_log_date, extract_log_file_name
from libs.lib_status import (
    compute_date_status,
    DATE_STATUS_PARTIAL,
    DATE_STATUS_QUEUE,
    LOG_FILE_STATUS_QUEUE,
    LOG_FILE_STATUS_LOADED, is_valid_log, LOG_FILE_STATUS_INVALID
)
from libs.lib_summary import parse_summary
from models.declarative import Base, DateStatus, LogFile, LogFileSummary
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound


def create_tables(database_uri):
    engine = create_engine(database_uri)
    Base.metadata.create_all(engine)


def get_db_session(database_uri):
    engine = create_engine(database_uri)

    Base.metadata.bind = engine
    db_session = sessionmaker(bind=engine)

    return db_session()


def get_recent_log_files(database_uri, collection, ignore_status_list):
    db_session = get_db_session(database_uri)
    return db_session.query(LogFile).filter(LogFile.collection == collection).filter(LogFile.status.notin_(ignore_status_list)).order_by(LogFile.date.desc())


def get_lines_parsed(database_uri, log_file_id):
    db_session = get_db_session(database_uri)
    try:
        query_result = db_session.query(LogFileSummary).filter(LogFileSummary.idlogfile == log_file_id)

        max_lines_parsed = 0

        for qr in query_result:
            if qr.lines_parsed > max_lines_parsed:
                max_lines_parsed = qr.lines_parsed

        return max_lines_parsed
    except NoResultFound:
        pass


def update_available_log_files(database_uri, dir_usage_logs, collection):
    db_session = get_db_session(database_uri)

    for root, dirs, files in os.walk(dir_usage_logs):
        for name in files:
            file = os.path.join(root, name)
            server = extract_log_server_name(file)
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

                    if not is_valid_log(lf.full_path, lf.server, lf.date):
                        lf.status = LOG_FILE_STATUS_INVALID
                        logging.warning('LogFile row created, but will not be loaded due to unmet requirements: %s' % file)
                    else:
                        logging.info('LogFile row created: (%s, %s)' % (lf.full_path, lf.created_at.strftime('%y-%m-%d %H:%M:%S')))

                    db_session.add(lf)
                    db_session.commit()
            else:
                logging.warning('LogFile ignored due to invalid date and server: %s' % file)


def update_log_file_status(database_uri, collection, file_id, status):
    db_session = get_db_session(database_uri)

    try:
        log_file_row = db_session.query(LogFile).filter(and_(LogFile.collection == collection,
                                                             LogFile.id == file_id)).one()
        if log_file_row.status != status:
            if log_file_row.status != LOG_FILE_STATUS_LOADED:
                logging.info('Changing status of control_log_file.id=%s from %s to %s' % (file_id, log_file_row.status, status))
                log_file_row.status = status
    except NoResultFound:
        pass
    except MultipleResultsFound:
        pass

    db_session.commit()


def update_date_status(database_uri, collection):
    db_session = get_db_session(database_uri)

    try:
        lfdate_to_status_files = {}
        for lf in db_session.query(LogFile).filter(LogFile.collection == collection):
            if lf.date not in lfdate_to_status_files:
                lfdate_to_status_files[lf.date] = []
            lfdate_to_status_files[lf.date].append(lf.status)

        for key in lfdate_to_status_files:
            new_status = compute_date_status(lfdate_to_status_files[key])

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
    except NoResultFound:
        logging.error('There are no log files registered in the table log_file')

    db_session.commit()


def update_log_file_summary(database_uri, summary_file, expected_lines, log_file_id):
    summary_data = parse_summary(summary_file, expected_lines)

    db_session = get_db_session(database_uri)

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
