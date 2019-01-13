from bs4 import BeautifulSoup
import requests
from models import Films

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from sqlalchemy.orm.exc import NoResultFound
from models import Films

from settings import psql


class FilmScraper:

    def __init__(self, psql):
        self.engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
                                psql['user'], psql['password'],
                                psql['host'], psql['port'],
                                psql['database']))

        self.Base = declarative_base()
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def scrape_film_titles_and_hrefs(self, latest_year, earliest_year):
    
        for year in range(latest_year, earliest_year - 1, -1):
            
            print("\nCollecting data for {} films\n".format(year))

            url = "https://en.wikipedia.org/wiki/{}_in_film".format(year)
            r = requests.get(url)
            html = BeautifulSoup(r.text, "lxml")

            tables = html.find_all("table", attrs={"class":"wikitable"})
            for table in tables:
                # film release tables start with "Opening" column
                if table.find("th").text.strip() == "Opening":
                    n_cols = len(table.find("tr").find_all('th'))
                    # every row in the table body is a released film
                    for row in table.find("tbody").find_all("tr"):
                        row_data = row.find_all("td")
                        try:
                            if len(row_data) == n_cols:
                                film = row_data[1]
                                title = film.text.strip()

                            elif len(row_data) == n_cols - 1:
                                film = row_data[0]
                                title = film.text.strip()

                            try:
                                exists = self.session.query(Films).\
                                    filter_by(title=title).one()
                                if exists:
                                    continue

                            except NoResultFound:
                                try:
                                    href = film.find('a')['href']

                                except TypeError:
                                    href = None
                                
                                film_obj = Films(
                                    title = title,
                                    wiki_href = href
                                    )
                                
                                self.session.add(film_obj)
                                self.session.commit()

                                print("Collected data for {}".\
                                        format(film_obj.title))

                        except Exception as e:
                            print(e)
                            self.session.rollback()

if __name__ == "__main__":
    
    s = FilmScraper(psql)
    s.scrape_film_titles_and_hrefs(2018, 2010)