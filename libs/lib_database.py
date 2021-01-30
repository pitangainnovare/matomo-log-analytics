import logging

from datetime import timedelta
from models.declarative import Base, DateStatus
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound


def create_tables(database_uri):
    engine = create_engine(database_uri)
    Base.metadata.create_all(engine)


def get_db_session(str_connection):
    engine = create_engine(str_connection)

    Base.metadata.bind = engine
    db_session = sessionmaker(bind=engine)

    return db_session()


def set_date_status(str_connection, date, status):
    db_session = get_db_session(str_connection)

    try:
        existing_date_status = db_session.query(DateStatus).filter(DateStatus.date == date).one()
        existing_date_status.status = status
    except NoResultFound:
        pass

    db_session.commit()


def extract_pretable(str_connection, date):
    currente_date = date
    next_date = date + timedelta(days=1)

    raw_query = 'SELECT server_time as serverTime, config_browser_name as browserName, config_browser_version as browserVersion, inet_ntoa(conv(hex(location_ip), 16, 10)) as ip, location_latitude as latitude, location_longitude as longitude, name as actionName from matomo_log_link_visit_action LEFT JOIN matomo_log_visit on matomo_log_visit.idvisit = matomo_log_link_visit_action.idvisit LEFT JOIN matomo_log_action on matomo_log_action.idaction = matomo_log_link_visit_action.idaction_url WHERE server_time >= "{0}" AND server_time < "{1}" ORDER BY ip;'.format(currente_date, next_date)

    engine = create_engine(str_connection)
    return engine.execute(raw_query)


def get_dates_able_to_extract(str_connection, collection, number_of_days):
    dates = []

    db_session = get_db_session(str_connection)
    try:
        ds_results = db_session.query(DateStatus).filter(DateStatus.collection == collection).filter(DateStatus.status == 'loaded').order_by(DateStatus.date).limit(number_of_days)

        for ds in ds_results:
            dates.append(ds.date)
    except NoResultFound:
        logging.info('There are no dates to be extracted')

    return dates
