# 2nd pass on scraping summary data from mojo for films that failed
# initial mojo-scrape.py. No attempts for daily data.

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from models import Films, MojoSummary, MojoDaily, FilmsWiki

from settings import psql


if __name__ == "__main__":
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
                                psql['user'], psql['password'],
                                psql['host'], psql['port'],
                                psql['database']))

    Base = declarative_base()
    Session = sessionmaker(bind=engine)
    session = Session()

    all_films = session.execute("""
                                select title
                                from films_wiki
                                left join mojo_summary
                                on films_wiki.id = mojo_summary.id
                                where mojo_summary.id is null;
                                """
                                )
    for title in all_films:
        print(title[0])
