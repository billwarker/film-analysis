from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (scoped_session, sessionmaker,
                            backref, relationship)
from sqlalchemy import (Column, DateTime, ForeignKey,
						Integer, String, Boolean, Float,
						func, BigInteger)

from settings import psql
from models import *

engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
						psql['user'], psql['password'],
						psql['host'], psql['port'],
						psql['database']))

Base = declarative_base()
Base.metadata.bind = engine

Session = sessionmaker(bind=engine)
session = Session()

for film in session.query(Films):
    print(film.wiki_href)

