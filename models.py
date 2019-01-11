from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (scoped_session, sessionmaker,
                            backref, relationship)
from sqlalchemy import (Column, DateTime, ForeignKey,
						Integer, String, Boolean, Float,
						func, BigInteger)

from settings import psql

engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
						psql['user'], psql['password'],
						psql['host'], psql['port'],
						psql['database']))

Base = declarative_base()
Base.metadata.bind = engine

class FilmsOMDB(Base):
    __tablename__ = 'films_omdb'

    id = Column(Integer, primary_key=True)

    # film information

    title = Column(String)
    year = Column(Integer)
    rated = Column(String)
    released = Column(DateTime)
    runtime = Column(Integer)
    
    genre_1 = Column(String)
    genre_2 = Column(String)
    genre_3 = Column(String)
    genre_4 = Column(String)
    genre_5 = Column(String)

    plot = Column(String)

    language_1 = Column(String)
    language_2 = Column(String)
    language_3 = Column(String)
    language_4 = Column(String)
    language_5 = Column(String)

    country_1 = Column(String)
    country_2 = Column(String)
    country_3 = Column(String)
    country_4 = Column(String)
    country_5 = Column(String)

    oscar_wins = Column(Integer)
    oscar_noms = Column(Integer)
    award_wins = Column(Integer)
    award_noms = Column(Integer)

    ratings_tomatoes = Column(Integer)
    ratings_meta = Column(Integer)
    ratings_imdb = Column(Float)

    dvd_release = Column(DateTime)
    box_office = Column(Integer)
    production = Column(String)

class FilmsWiki(Base):
    __tablename__ = 'films_wiki'

    id = Column(Integer, primary_key=True)


class PeopleByFilm(Base):
    __tablename__ = 'people_by_film'

    id = Column(Integer, primary_key=True)

    # people by film
    film_title = Column(String)
    person = Column(String)
    role = Column(String)

class People(Base):
    __tablename__ = 'people'

    id = Column(Integer, primary_key=True)

def init_db():
    Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    response = input("Drop all tables? [y]")
    if response == 'y':
        FilmsOMDB.__table__.drop()
        FilmsWiki.__table__.drop()
        PeopleByFilm.__table__.drop()
        People.__table__.drop()
    
    reponse = input("Create all tables? [y]")
    if reponse == 'y':
        print("Creating database")
        init_db()
