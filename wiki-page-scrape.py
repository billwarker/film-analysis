from bs4 import BeautifulSoup
import unicodedata
import bs4
import requests
import re
import time

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (scoped_session, sessionmaker,
                            backref, relationship)

from models import (FilmsWiki, Persons, FilmPersons, Countries,
                    FilmCountries, Companies, FilmCompanies,
                    Languages, FilmLanguages, Genres,
                    FilmGenres)

from settings import psql


def parse_yearly_film_hrefs(year):
    url = "https://en.wikipedia.org/wiki/{}_in_film".format(year)
    r = requests.get(url)
    html = BeautifulSoup(r.text, "lxml")
    film_hrefs = []

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
                        film = row_data[1].find('a')["href"]
                        film_hrefs.append(film)
                    elif len(row_data) == n_cols - 1:
                        film = row_data[0].find('a')["href"]
                        film_hrefs.append(film)
                except Exception:
                    pass
    return film_hrefs

def redirect_to_film_page(href):
    url = "https://en.wikipedia.org{}".format(href)
    r = requests.get(url)
    html_soup = BeautifulSoup(r.text, "lxml")
    return html_soup

infobox_attrs = {
    "Title": "Title",
    "Music by": "Composer",
    "Produced by": "Producer",
    "Edited by": "Editor",
    "Cinematography": "Cinematographer",
    "Release date": "Released",
    "Based on": "Source Material",
    "Budget": "Budget",
    "Story by": "Story",
    "Country": "Country",
    "Productioncompany": "Production",
    "Starring": "Actor",
    "Directed by": "Director",
    "Narrated by": "Narrator",
    "Written by": "Writer",
    "Running time": "Running Time",
    "Language": "Language",
    "Box office": "Box Office",
    "Distributed by": "Distribution",
    "Screenplay by": "Screenwriter",
    "Productioncompanies": "Production"
    }

def parse_html_breaks(row_data):
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

def parse_html_lists(row_data):
    data = list()
    lines = row_data.find_all("li")
    for li in lines:
        if li.text != '':
            data.append(li.text)
    return data

def clean_infobox_string(string):
    new_str = unicodedata.normalize("NFKD", string)
    new_str =re.sub(r"\[[0-9]]", '', new_str)

    if '\n' in new_str:
        new_str = new_str.split('\n')
    return new_str

def scrape_film_infobox(html_soup):
    try:
        infobox_data = dict.fromkeys(set(infobox_attrs.values()), None)
        infobox = html_soup.find("table", attrs={"class":"infobox"})
        infobox_rows = infobox.find_all("tr")
        infobox_data["Title"] = infobox_rows[0].text

        for row in infobox_rows:
            try:
                row_desc = row.find("th").text.strip()
                row_data = row.find("td")

                if row_data.find_all("li"):
                    data = parse_html_lists(row_data)
                    clean_data = [clean_infobox_string(item) for item in data]
                
                elif row_data.find_all("br"):
                    data = parse_html_breaks(row_data)
                    clean_data = [clean_infobox_string(item) for item in data]

                else:
                    clean_data = clean_infobox_string(row_data.text)
                
                infobox_data[infobox_attrs[row_desc]] = clean_data
            except Exception:
                pass

        return infobox_data

    except Exception as e:
        print(e)

def main():
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
						psql['user'], psql['password'],
						psql['host'], psql['port'],
						psql['database']))
    
    Base = declarative_base()
    Session = sessionmaker(bind=engine)
    session = Session()
    year_hrefs = parse_yearly_film_hrefs(2017)
    for href in year_hrefs:
        film_page = redirect_to_film_page(href)
        data = scrape_film_infobox(film_page)
        
        # load films_wiki

        film_obj = FilmsWiki(
            title = data["Title"],
            released = data["Released"],
            based_on = data["Source Material"],
            running_time = data["Running Time"],
            budget = data["Budget"],
            box_office = data["Box Office"],
            )
        session.add(film_obj)

        # load persons and film_persons
        roles = ["Actor", "Director", "Writer", "Screenwriter",
                 "Editor", "Producer", "Composer", "Narrator",
                 "Story"]

        for role in roles:
            if data[role]:
                persons = data[role]
                
                # if single person, make list
                if type(persons) != list: 
                    persons = [persons] 

                for name in persons:
                    person_obj = Persons(
                        full_name = name
                        )
                    film_person_obj = FilmPersons(
                        film = film_obj,
                        person = person_obj,
                        role = role
                        )
                    session.add(film_person_obj)
        
        # load countries and film_counries
        if data["Country"]:
            countries = data["Country"]
            
            # if single country, make list
            if type(countries) != list:
                countries = [countries]
            for country in countries:
                country_obj = Countries(
                    country = country
                    )
                film_country_obj = FilmCountries(
                    film = film_obj,
                    country = country_obj
                    )
                session.add(film_country_obj)

        # load companies and film_companies
        roles = ["Production", "Distribution"]
        for role in roles:
            if data[role]:
                companies = data[role]

                # if single company, make list
                if type(companies) != list:
                    companies = [companies]

                for company in companies:
                    company_obj = Companies(
                        company = company
                        )
                    film_company_obj = FilmCompanies(
                        film = film_obj,
                        company = company_obj,
                        role = role
                    )
                    session.add(film_company_obj)

        # load languages and film_languages
        if data["Language"]:
            languages = data["Language"]
            
            # if single language, make list
            if type(languages) != list:
                languages = [languages]
            for language in languages:
                language_obj = Languages(
                    language = language
                    )
                film_language_obj = FilmLanguages(
                    film = film_obj,
                    language = language_obj
                    )
                session.add(film_language_obj)
    session.commit()

