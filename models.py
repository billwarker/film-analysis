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

### wikipedia core

class FilmsWiki(Base):
    __tablename__ = 'films_wiki'

    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    released = Column(DateTime, nullable=False)
    based_on = Column(String)
    story_by = Column(String)
    running_time = Column(Integer)
    budget = Column(Integer)
    box_office = Column(Integer)
    
    people = relationship(
        "Persons",
        secondary="film_persons"
    )

    countries = relationship(
        "Countries",
        secondary="film_countries"
    )

    companies = relationship(
        "Companies",
        secondary="film_companies"
    )

    languages = relationship(
        "Languages",
        secondary="film_languages"
    )

class Persons(Base):
    __tablename__ = 'persons'

    id = Column(Integer, primary_key=True)
    full_name = Column(String)
    films = relationship(
        "FilmsWiki",
        secondary="film_persons"
        )

class FilmPersons(Base):
    
    __tablename__ = 'film_persons'
    # people by film
    film_id = Column(Integer, ForeignKey('films_wiki.id'), primary_key=True)
    film = relationship("FilmsWiki", backref=backref("film_link"))
    person_id = Column(Integer, ForeignKey('persons.id'), primary_key=True)
    person = relationship("Persons", backref=backref("person_link"))
    role = Column(String, primary_key=True)

class Countries(Base):

    __tablename__ = 'countries'
    id = Column(Integer, primary_key=True)
    country = Column(String)
    films = relationship(
        FilmsWiki,
        secondary="film_countries"
        )

class FilmCountries(Base):

    __tablename__ = 'film_countries'
    film_id = Column(Integer, ForeignKey('films_wiki.id'), primary_key=True)
    film = relationship("FilmsWiki", backref=backref("film_link"))
    country_id = Column(Integer, ForeignKey('countries.id'), primary_key=True)
    country = relationship("Countries", backref=backref("country_link"))

class Companies(Base):

    __tablename__ = 'companies'
    id = Column(Integer, primary_key=True)
    company = Column(String, nullable=False)
    films = relationship(
        FilmsWiki,
        secondary="film_companies"
        )

class FilmCompanies(Base):

    __tablename__ = 'film_companies'
    film_id = Column(Integer, ForeignKey('films_wiki.id'), primary_key=True)
    film = relationship("FilmsWiki", backref=backref("film_link"))
    company_id = Column(Integer, ForeignKey('companies.id'), primary_key=True)
    company = relationship("Companies", backref=backref("company_link"))
    role = Column(String, primary_key=True)

class Languages(Base):

    __tablename__ = 'languages'
    id = Column(Integer, primary_key=True)
    language = Column(String)
    films = relationship(
        FilmsWiki,
        secondary="film_languages"
    )

class FilmLanguages(Base):

    __tablename__ = 'film_languages'
    film_id = Column(Integer, ForeignKey('films_wiki.id'), primary_key=True)
    film = relationship("FilmsWiki", backref=backref("film_link"))
    language_id = Column(Integer, ForeignKey('languages.id'), primary_key=True)
    language = relationship("Languages", backref=backref("language_link"))

class Genres(Base):

    __tablename__ = 'genres'
    id = Column(Integer, primary_key=True)
    genre = Column(String)
    films = relationship(
        FilmsWiki,
        secondary="film_genres"
    )

class FilmGenres(Base):

    __tablename__ = 'film_genres'
    film_id = Column(Integer, ForeignKey('films_wiki.id'), primary_key=True)
    film = relationship("FilmsWiki", backref=backref("film_link"))
    genre_id = Column(Integer, ForeignKey('genres.id'), primary_key=True)
    genre = relationship("Genres", backref=backref("genre_link"))

def init_db():
    Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    init_db()
    response = input("Drop all tables? [y]")
    if response == 'y':
        #FilmsOMDB.__table__.drop()
        for table in Base.metadata.tables.keys():
            engine.execute("DROP TABLE %s CASCADE;" % table)
        # FilmsWiki.__table__.drop()
        # FilmPersons.__table__.drop()
        # Persons.__table__.drop()
        # FilmCountries.__table__.drop()
        # Countries.__table__.drop()
        # FilmCompanies.__table__.drop()
        # Companies.__table__.drop()
        # FilmLanguages.__table__.drop()
        # Languages.__table__.drop()
    
    reponse = input("Create all tables? [y]")
    if reponse == 'y':
        print("Creating database")
        init_db()
