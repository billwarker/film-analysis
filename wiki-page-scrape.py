from bs4 import BeautifulSoup
import unicodedata
import bs4
import requests
import re
import datetime
import time

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (scoped_session, sessionmaker,
                            backref, relationship)
from sqlalchemy.orm.exc import NoResultFound

from models import (Films, FilmsWiki, Persons, FilmPersons, Countries,
                    FilmCountries, Companies, FilmCompanies,
                    Languages, FilmLanguages, Genres,
                    FilmGenres)

from settings import psql

class WikiFilmScrape:

    infobox_attrs = {"Title": "Title", "Music by": "Composer",
                     "Produced by": "Producer", "Edited by": "Editor",
                     "Cinematography": "Cinematographer",
                     "Release date": "Released",
                     "Based on": "Source Material", "Budget": "Budget",
                     "Story by": "Story", "Country": "Country",
                     "Productioncompany": "Production",
                     "Starring": "Actor", "Directed by": "Director",
                     "Narrated by": "Narrator", "Written by": "Writer",
                     "Running time": "Running Time", "Language": "Language",
                     "Box office": "Box Office",
                     "Distributed by": "Distribution",
                     "Screenplay by": "Screenwriter",
                     "Productioncompanies": "Production"
                    }

    person_roles = ["Actor", "Director", "Writer", "Screenwriter",
                    "Editor", "Producer", "Composer", "Narrator",
                    "Story"]
    
    company_roles = ["Production", "Distribution"]

    def __init__(self, psql):
        self.engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
                                psql['user'], psql['password'],
                                psql['host'], psql['port'],
                                psql['database']))

        self.Base = declarative_base()
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def redirect_to_film_page(self, href):
        url = "https://en.wikipedia.org{}".format(href)
        r = requests.get(url)
        html_soup = BeautifulSoup(r.text, "lxml")
        return html_soup

    ### parsing infobox functions

    def parse_html_breaks(self, row_data):
        data = list()
        breaks = row_data.find_all("br")
        for br in breaks:
            item = br.previous_sibling
            if type(item) == bs4.element.NavigableString:
                data.append(item.strip())
            elif type(item) == bs4.element.Tag:
                data.append(item.text.strip())
    
        last_item = breaks[-1].next_sibling
        if type(last_item) == bs4.element.NavigableString:
            data.append(last_item.strip())
        elif type(last_item) == bs4.element.Tag:
            data.append(last_item.text.strip())

        return data

    def parse_html_lists(self, row_data):
        data = list()
        lines = row_data.find_all("li")
        for li in lines:
            if li.text != '':
                data.append(li.text)
        return data

    def clean_infobox_string(self, string):
        new_str = unicodedata.normalize("NFKD", string)
        new_str =re.sub(r"\[[0-9]]", '', new_str)

        if '\n' in new_str:
            new_str = new_str.split('\n')
        return new_str

    # full function for scraping infobox

    def scrape_film_infobox(self, html_soup):
        try:
            infobox_data = dict.fromkeys(set(self.infobox_attrs.values()), None)
            infobox = html_soup.find("table", attrs={"class":"infobox"})
            infobox_rows = infobox.find_all("tr")
            infobox_data["Title"] = infobox_rows[0].text

            for row in infobox_rows:
                try:
                    row_desc = row.find("th").text.strip()
                    row_data = row.find("td")

                    if row_data.find_all("li"):
                        data = self.parse_html_lists(row_data)
                        clean_data = [self.clean_infobox_string(item) for item in data]
                    
                    elif row_data.find_all("br"):
                        data = self.parse_html_breaks(row_data)
                        clean_data = [self.clean_infobox_string(item) for item in data]

                    else:
                        clean_data = self.clean_infobox_string(row_data.text)
                    
                    infobox_data[self.infobox_attrs[row_desc]] = clean_data
                except Exception:
                    pass

            return infobox_data

        except Exception as e:
            print(e)


    # functions for formatting data

    def extract_money(self, money_data):
        drop_hyphen_pattern = r"\w*(-.* )"
        million_pattern = r".*\$([0-9]*.*) million"
        billion_pattern = r".*\$([0-9]*.*) billion"
        million = 1000000
        billion = 1000000000
        try:
            money_data = re.sub(drop_hyphen_pattern, '', money_data)
            for pattern, num in zip([million_pattern, billion_pattern],
                                [million, billion]):
                pattern = re.compile(pattern)
                regex = re.match(pattern, money_data)
                if regex.group(1):
                    value = float(regex.group(1))
                    return int(value * num)

        except Exception:
            pass

    def extract_date(self, release_data):
        try:
            date_pattern = r".*\((20[0-9]+-[0-9]+-[0-9]+)\)"
            # first, try to find USA release
            for date_str in release_data:
                if "United States" in date_str or "USA" in date_str:
                    date = re.match(date_pattern, date_str).group(1)
                    datetime_obj = datetime.datetime.strptime(date, "%Y-%m-%d")
                    return datetime_obj.date()
            # if not just take the first date in the list
            date = re.match(date_pattern, release_data[0]).group(1)
            datetime_obj = datetime.datetime.strptime(date, "%Y-%m-%d")
            return datetime_obj.date()

        except Exception:
            pass

    def extract_runtime(self, runtime_data):
        try:
            runtime = int(runtime_data.split(' ')[0])
            return runtime
        except Exception:
            pass

    def main(self):

        print("Scraping data for all films...")
        all_films = self.session.query(Films)

        for film in all_films:
            try:
                film_page = self.redirect_to_film_page(film.wiki_href)
                film_data = self.scrape_film_infobox(film_page)
            except Exception as e:
                print(e)
                continue
                
            try:
                # 1. load films_wiki
                film_obj = FilmsWiki(

                    film = film,
                    title = film_data["Title"],
                    released = self.extract_date(film_data["Released"]),
                    running_time = self.extract_runtime(film_data["Running Time"]),
                    budget = self.extract_money(film_data["Budget"]),
                    box_office = self.extract_money(film_data["Box Office"]),
                    )
                
                self.session.add(film_obj)

                # 2. load persons and film_persons

                for role in self.person_roles:
                    if film_data[role]:
                        persons = film_data[role]
                        
                        # if single person, make list
                        if type(persons) != list: 
                            persons = [persons] 

                        for name in persons:
                            try:
                                existing_person = self.session.query(Persons).\
                                                filter_by(full_name=name).one()
                                person_obj = existing_person

                            except Exception:
                                try:
                                    person_obj = Persons(
                                        full_name = name
                                        )
                                    self.session.add(person_obj)
                                    self.session.flush()
                                
                                except Exception:
                                    self.session.rollback()
                                
                            try:
                                film_person_obj = FilmPersons(
                                    film = film_obj,
                                    person = person_obj,
                                    role = role
                                    )
                                self.session.add(film_person_obj)
                                self.session.flush()
                            except Exception:
                                self.session.rollback()


                # 3. load countries and film_counries
                if film_data["Country"]:
                    countries = film_data["Country"]
                    
                    # if single country, make list
                    if type(countries) != list:
                        countries = [countries]

                    for country in countries:
                        try:
                            existing_country = self.session.query(Countries).\
                                            filter_by(country=country).one()
                            country_obj = existing_country

                        except Exception:
                            try:
                                country_obj = Countries(
                                    country = country
                                    )
                                self.session.add(country_obj)
                                self.session.flush()
                            except Exception:
                                self.session.rollback()

                        try:       
                            film_country_obj = FilmCountries(
                                film = film_obj,
                                country = country_obj
                                )
                            self.session.add(film_country_obj)
                            self.session.flush()
                        except Exception:
                            self.session.rollback()

                # 4. load companies and film_companies
                for role in self.company_roles:
                    if film_data[role]:
                        companies = film_data[role]

                        # if single company, make list
                        if type(companies) != list:
                            companies = [companies]

                        for company in companies:
                            try:
                                existing_company = self.session.query(Companies).\
                                                filter_by(company=company).one()
                                company_obj = existing_company
                            
                            except Exception:
                                try:
                                    company_obj = Companies(
                                        company = company
                                        )
                                    self.session.add(company_obj)
                                    self.session.flush()

                                except Exception:
                                    self.session.rollback()
                            try:
                                film_company_obj = FilmCompanies(
                                    film = film_obj,
                                    company = company_obj,
                                    role = role
                                    )
                                self.session.add(film_company_obj)
                                self.session.flush()
                            except Exception:
                                self.session.rollback()

                # 5. load languages and film_languages
                if film_data["Language"]:
                    languages = film_data["Language"]
                    
                    # if single language, make list
                    if type(languages) != list:
                        languages = [languages]

                    for language in languages:
                        try:
                            existing_language = self.session.query(Languages).\
                                                filter_by(language=language).one()
                            language_obj = existing_language

                        except Exception:
                            try:
                                language_obj = Languages(
                                    language = language
                                    )
                                self.session.add(language_obj)
                                self.session.flush()
                            except Exception:
                                self.session.rollback()

                        try:
                            film_language_obj = FilmLanguages(
                                film = film_obj,
                                language = language_obj
                                )
                            self.session.add(film_language_obj)
                            self.session.flush()
                        except Exception:
                            self.session.rollback()

                    print("Collected data for {}".format(film.title))
                    self.session.commit()
            
            except Exception as e:
                print(e)
                continue

if __name__ == "__main__":
    s = WikiFilmScrape(psql)
    s.main()



# def load_fields_and_associations(data, Model, Association, roles=None):
#     if roles:
#         for role in roles:
#             if data[role]:
#                 data_obj = data[role]
            
#             # if single object, make list
#             if type(data_obj) != list:
#                 data_obj = [data_obj]
            
#             for key in data_obj:
#                 try:
#                     already_exists = session.query(Model).\
#                                      filter_by(Model.__table__.columns[1] = key)
