from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from models import Companies, Countries, Languages, Persons
from models import (FilmsWiki, FilmCompanies, FilmCountries, FilmLanguages,
                    FilmPersons)

from settings import psql
import datetime

engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
                                psql['user'], psql['password'],
                                psql['host'], psql['port'],
                                psql['database']))

Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()

date = datetime.datetime(2015,8,14)

film_obj = FilmsWiki(
    title = "Yeet",
    released = date,
    based_on = None,
    story_by = None,
    running_time = 30,
    budget = 5000,
    box_office = 50000,

)

person_obj = Persons(
    full_name = "Sweeterman"
)

person2_obj = Persons(
    full_name = "Hypedude"
)

# company = Companies(
#     company = "Yuh Productions"
# )

# language = Languages(
#     language = 'English'
# )

film_persons = FilmPersons(person = person_obj,
                           film = film_obj,
                           role = "Director")
film_persons2 = FilmPersons(person = person2_obj,
                           film = film_obj,
                           role = "Producer")


session.add(film_persons)
session.add(film_persons2)
session.commit()

