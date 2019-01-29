# 2nd pass on scraping data from mojo for films that failed
# initial mojo-scrape.py.

from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from models import Films, MojoSummary, MojoDaily, FilmsWiki

from settings import psql
from mojo_scrape import *


if __name__ == "__main__":
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
                                psql['user'], psql['password'],
                                psql['host'], psql['port'],
                                psql['database']))

    Base = declarative_base()
    Session = sessionmaker(bind=engine)
    session = Session()

    all_films = session.query(FilmsWiki)\
                .outerjoin(MojoSummary)\
                .filter(MojoSummary.id == None).all()

    for film in all_films:
        try:
            time.sleep(2)
            print(film.title)
            search_page = search_for_film(film.title)
            href = parse_search_page_for_href(search_page, film)
            print(href)
            if href:
                # extract mojo summary
                time.sleep(2)
                summary_data = extract_mojo_summary(href)
                mojo_summary_obj = MojoSummary(
                    film = film,
                    domestic_gross = summary_data["Domestic"],
                    foreign_gross = summary_data["Foreign"],
                    budget = summary_data["Production Budget"]
                    )
                session.add(mojo_summary_obj)
                session.commit()
                print("Collected summary data for {}".format(film.title))

        except Exception as e:
            session.rollback()
            print("Problem with summary data for {}: {}"\
                  .format(film.title, e))

        try:
            film_id = extract_film_id(href)
            time.sleep(2)
            daily_data = extract_mojo_daily(film_id)
            for data in daily_data:
                mojo_daily_obj = MojoDaily(
                    film = film,
                    date = data["Date"],
                    day = data["Day"],
                    rank = data["Rank"],
                    gross = data["Gross"],
                    num_theatres = data["Theatres"],
                    num_day = data["Day #"]
                    )
                session.add(mojo_daily_obj)
            
            session.commit()
            print("Collected daily data for {}".format(film.title))

        except Exception as e:
            session.rollback()
            print("Problem with daily data for {}: {}"\
                  .format(film.title, e))