from bs4 import BeautifulSoup
import bs4
import requests
import re
import datetime
import time
import unicodedata

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (scoped_session, sessionmaker,
                            backref, relationship)
from sqlalchemy.orm.exc import NoResultFound
from models import Films, MojoSummary, MojoDaily, FilmsWiki

from settings import psql

engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
                                psql['user'], psql['password'],
                                psql['host'], psql['port'],
                                psql['database']))

Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()

# for film in all_films:
#     print(film.title)

def search_for_film(title):
    query = title.replace(" ", "%20")
    query = query.replace(",", "")
    query = query.replace(":", "")
    query = query.replace("'", "")

    print(query)
    url = "https://www.boxofficemojo.com/search/?q={}".format(query)
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "lxml")
    return soup

def parse_search_page_for_href(soup, film):
    try:
        results_table = soup.find_all("table",
                                attrs={"cellpadding":5})[1]
    except IndexError:
        try:
             results_table = soup.find_all("table",
                                attrs={"cellpadding":5})[0]
        except IndexError:
            return None   

    for row in results_table.find_all("tr")[1:]:
        correct_href = find_correct_href(row, film.title, film.released.year)
        if correct_href:
            return correct_href

def find_correct_href(row, film_title, film_year):
    try:

        title_cell = row.find_all("td")[0]
        date_cell = row.find_all("td")[6]
        
        title_text = title_cell.text.strip()
        print(title_text)
        # no colons in either film_title or text_title
        title_text = title_text.replace(':', '')
        film_title = film_title.replace(':', '')
        # make both lowercase to avoid casing discrepencies
        title_text = title_text.lower()
        film_title = film_title.lower()

        title_with_year = "{} ({})".format(film_title, film_year)
        title_with_last_year = "{} ({})".format(film_title, film_year - 1)
        title_with_next_year = "{} ({})".format(film_title, film_year + 1)

        try:
            date = datetime.datetime.strptime(date_cell.text, "%m/%d/%Y")
            year = date.year

            # title with year being +/- 1
            if title_text == film_title and (year == film_year or year == (film_year + 1) or year == (film_year - 1)):
                return title_cell.find('a')['href']

            # "title (year)" with year being +/- 1    
            elif title_text == title_with_year or title_text == title_with_last_year or title_text == title_with_next_year:
                return title_cell.find('a')['href']
        
        # no date so just title match
        except ValueError:      
            if title_text == film_title:
                return title_cell.find('a')['href']

    except IndexError:
        pass
    

# def scrape(film):
#     search_page = search_for_film(film.title)
#     time.sleep(2)
#     href = parse_search_page_for_href(search_page, film)
#     print(href)

# all_films = session.query(FilmsWiki)
# for film in all_films:
#     scrape(film)

# film = session.query(FilmsWiki).filter_by(title="The Strangers: Prey at Night").one()
# print(film.title)
# print(film.released.year)

# page = search_for_film(film.title)
# href = parse_search_page_for_href(page, film)
# print(href)

#href = "/movies/?id=paranormalactivity2.htm"
href = '/movies/?id=pacificrim2.htm'
#href = '/movies/?id=flower.htm'

def clean_value(value_str):
    value = unicodedata.normalize("NFKD", value_str)
    value = value.replace('$', '')
    value = value.replace(',', '')

    return int(value)

def clean_budget(budget_str):
    budget_str = unicodedata.normalize("NFKD", budget_str)
    million_pattern = re.compile(r".*\$([0-9]*) million")
    try:
        regex = re.match(million_pattern, budget_str)
        match = regex.groups()[0]
        if match:
            budget = int(match) * 1000000
            return budget
    except AttributeError:
        try:            
            thousands_pattern = re.compile(r".*\$([0-9,]*)")
            regex = re.match(thousands_pattern, budget_str)
            match = regex.groups()[0]
            if match:
                budget = int(match.replace(',', ''))
                return budget
        except AttributeError:
            return None

def extract_mojo_summary(href):
    summary_dict = {"Domestic": None,
                    "Foreign": None,
                    "Production Budget": None}
    
    url = "https://www.boxofficemojo.com{}".format(href)
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "lxml")

    # domestic and foreign
    mp_box = soup.find("div", attrs={"class":"mp_box"})
    grosses_table = mp_box.find("table")
    rows = grosses_table.find_all("tr")
    for row in rows:
        try:
            category = row.find_all("td")[0].text
            value = clean_value(row.find_all("td")[1].text)
            for key in list(summary_dict.keys()):
                if key in category:
                    summary_dict[key] = value
        except IndexError:
            pass

    # production budget
    summary_table = soup.find("table", attrs={"cellspacing":"1", "cellpadding":"4", "width":"95%"})
    rows = summary_table.find_all("tr")
    budget = clean_budget(rows[3].find_all("td")[1].text)
    summary_dict["Production Budget"] = budget

    return summary_dict

extract_mojo_summary(href)