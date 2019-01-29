from bs4 import BeautifulSoup
import bs4
import requests
import re
import datetime
import time
import unicodedata
import pickle

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from models import Films, MojoSummary, MojoDaily, FilmsWiki

from settings import psql

def search_for_film(title):
    query = title.replace(" ", "%20")
    query = title.replace("'", "%27")
    query = query.replace(",", "")
    #query = query.replace(":", "")
    query = query.replace("'", "")

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

def clean_value(value_str):
    value_str = value_str.strip()
    if value_str != "n/a":
        value = unicodedata.normalize("NFKD", value_str)
        value = value.replace('$', '')
        value = value.replace(',', '')

        return int(value)
    else:
        return None

def clean_money(money_str):
    money_str = unicodedata.normalize("NFKD", money_str)
    million_pattern = re.compile(r".*\$([0-9]*) million")
    try:
        regex = re.match(million_pattern, money_str)
        match = regex.groups()[0]
        if match:
            money = int(match) * 1000000
            return money
    except AttributeError:
        try:            
            thousands_pattern = re.compile(r".*\$([0-9,]*)")
            regex = re.match(thousands_pattern, money_str)
            match = regex.groups()[0]
            if match:
                money = int(match.replace(',', ''))
                return money
        except AttributeError:
            return None

def extract_mojo_summary(href):
    summary_data = {"Domestic": None,
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
            for key in list(summary_data.keys()):
                if key in category:
                    summary_data[key] = value
        except IndexError:
            pass

    # production budget
    summary_table = soup.find("table", attrs={"cellspacing":"1", "cellpadding":"4", "width":"95%"})
    rows = summary_table.find_all("tr")
    budget = clean_money(rows[3].find_all("td")[1].text)
    summary_data["Production Budget"] = budget

    return summary_data

def extract_film_id(href):
    pattern = re.compile(r".*id=(.*\.htm)")
    return re.match(pattern, href).group(1)

def clean_rank(rank_str):
    try:
        return int(rank_str)
    except ValueError:
        return None

def clean_date(date_str):
    pattern = re.compile("([A-Za-z.\t]*) ([0-9]*), ([0-9]*)")
    date_match = re.match(pattern, date_str)
    month = date_match.group(1).replace(".", "")
    month = month.strip()
    day = date_match.group(2)
    if len(day) < 2:
        day = "0" + day
    year = date_match.group(3)
    formatted = "{} {} {}".format(month, day, year)
    datetime_obj = datetime.datetime.strptime(formatted, "%b %d %Y")
    return datetime_obj

def extract_mojo_daily(film_id):

    daily_data = []

    url = "https://www.boxofficemojo.com/movies/?page=daily&view=chart&id={}".format(film_id)
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")

    daily_table = soup.find("table", attrs={"class":"chart-wide"})
    rows = daily_table.find_all("tr")
    for row in rows:
        row_data = row.find_all("td")
        if len(row_data) == 10:
            data = dict()
            data["Date"] = clean_date(row_data[1].text)
            data["Day"] = row_data[0].text
            data["Rank"] = clean_rank(row_data[2].text)
            data["Gross"] = clean_money(row_data[3].text)
            data["Theatres"] = int(row_data[6].text.replace(',',''))
            data["Day #"] = int(row_data[9].text)
            
            daily_data.append(data)
    
    return daily_data

if __name__ == "__main__":
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
                                psql['user'], psql['password'],
                                psql['host'], psql['port'],
                                psql['database']))

    Base = declarative_base()
    Session = sessionmaker(bind=engine)
    session = Session()

    all_films = session.query(FilmsWiki)
    success_pct = 0
    attempted = 0
    success = 0
    
    # main loop
    for film in all_films:
        attempted += 1
        try:
            search_page = search_for_film(film.title)
            href = parse_search_page_for_href(search_page, film)
            if href:
                film_id = extract_film_id(href)       
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

                # extract mojo dailies
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
                print("Collected data for {}".format(film.title))
                success += 1
                success_pct = success/attempted
                print("Success Rate: {}".format(round(success_pct, 3)))
                
        # if anything goes wrong show me error and rollback
        except Exception as e:
            session.rollback()
            print("Problem with {}: {}".format(film.title, e))
            success_pct = success/attempted
            print("Success Rate: {}".format(round(success_pct, 4)))