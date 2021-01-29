from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.declarative import Base


def create_tables(database_uri):
    engine = create_engine(database_uri)
    Base.metadata.create_all(engine)


def get_db_session(matomo_db_uri):
    engine = create_engine(matomo_db_uri)

    Base.metadata.bind = engine
    db_session = sessionmaker(bind=engine)

    return db_session()
