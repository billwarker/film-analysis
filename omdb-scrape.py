import requests
from bs4 import BeautifulSoup
import json
import pickle

import time
from datetime import datetime
import re
from numpy import nan

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from models import FilmsOMDB, Genres, FilmGenres

from settings import psql, api_key


class OMDB_Scraper:

    def __init__(self, psql, api_key):
        self.engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
                                psql['user'], psql['password'],
                                psql['host'], psql['port'],
                                psql['database']))

        self.Base = declarative_base()
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self.api_key = api_key

    def create_api_call(self, title):
        formatted_title = title.replace(" ", "+")
        api_call = 'http://www.omdbapi.com/?t={}&apikey={}'.format(formatted_title,
                                                                   self.api_key)
        return api_call

    def get_json_response(self, api_call):
        r = requests.get(api_call)
        json_str = BeautifulSoup(r.text, "lxml").find('p').text
        json_response = json.loads(json_str)
        return json_response

    ### functions for parsing json 

    def release_date_format(self, date_str):
        return datetime.strptime(date_str, "%d %b %Y")

    def dvd_release_date_format(self, dvd_date_str):
        if dvd_date_str == 'N/A': return None
        else:
            return datetime.strptime(dvd_date_str, "%d %b %Y")

    def runtime_format(self, runtime_str):
        return int(runtime_str.split(' ')[0])

    def tomato_rating(self, ratings_list):
        for rating in ratings_list:
            if rating["Source"] == 'Rotten Tomatoes': 
                return rating["Value"].replace('%', '')
        return None

    def meta_rating(self, rating_str):
        if rating_str == 'N/A': return None
        else: return int(rating_str)

    def imdb_rating(self, rating_str):
        if rating_str == 'N/A': return None
        else: return float(rating_str)

    def get_genres(self, genre_str):
        genres = genre_str.split(", ")
        if type(genres) == list:
            return genres

        return [genres]

    # def get_languages(self, language_str):
    #     languages = language_str.split(", ")
    #     if len(languages) > 5:
    #         languages = languages[:5]
    #     while len(languages) < 5:
    #         languages.append(nan)
    #     return languages

    # def get_countries(self, countries_str):
    #     countries = countries_str.split(", ")
    #     if len(countries) > 5:
    #         countries = countries[:5]
    #     while len(countries) < 5:
    #         countries.append(nan)
    #     return countries

    def box_office_format(self, box_office_str):
        if box_office_str == "N/A": return None
        else:
            box_office = box_office_str.replace(',','')
            box_office = box_office.replace('$', '')
            return int(box_office)

    def get_awards(self, awards_str):

        win_pattern = ".* ([0-9]*) win"
        nom_pattern = ".* ([0-9]*) nomination"
        oscar_win_pattern = ".*won ([0-9*]) oscar"
        oscar_nom_pattern = ".*nominated for ([0-9]*) oscar"
        patterns = [win_pattern, nom_pattern,
                    oscar_win_pattern, oscar_nom_pattern]

        awards = ["award_wins", "award_noms",
                  "oscar_wins", "oscar_noms"]

        awards_dict = {"oscar_wins": 0,
                       "oscar_noms": 0,
                       "award_wins": 0,
                       "award_noms": 0}

        if awards_str == "N/A":
            return awards_dict
        else:
            awards_str = awards_str.lower()
            for pattern, award in zip(patterns, awards):
                regex = re.compile(pattern)
                match_obj = regex.match(awards_str)
                if match_obj:
                    awards_dict[award] = int(match_obj.group(1))

        return awards_dict
    
    ### commit to db

    def commit(self):
        self.session.commit()

    ### main scraping function    

    def scrape_omdb(self, film_dict):
        years = list(film_dict.keys())
        for year in years:
            films = film_dict[year]
            print("Scraping the OMDB for all films released in {}\n".format(year))
            for title in films:
                try:
                    api_call = self.create_api_call(title)
                    data = self.get_json_response(api_call)
                    if data["Response"] == "True":

                        genres = self.get_genres(data["Genre"])
                        # languages = self.get_languages(data["Language"])
                        # countries = self.get_countries(data["Country"])
                        awards = self.get_awards(data["Awards"])
                        
                        film_omdb_obj = FilmsOMDB(
                            title = data["Title"],
                            year = int(data["Year"]),
                            rated = data["Rated"],
                            released = self.release_date_format(data["Released"]),
                            runtime = self.runtime_format(data["Runtime"]),

                            plot = data["Plot"],

                            oscar_wins = awards["oscar_wins"],
                            oscar_noms = awards["oscar_noms"],
                            award_wins = awards["award_wins"],
                            award_noms = awards["award_noms"],

                            ratings_tomatoes = self.tomato_rating(data["Ratings"]),
                            ratings_meta = self.meta_rating(data["Metascore"]),
                            ratings_imdb = self.imdb_rating(data["imdbRating"]),
                            
                            dvd_release = self.dvd_release_date_format(data["DVD"]),
                            box_office = self.box_office_format(data["BoxOffice"]),
                            )
                        self.session.add(film_omdb_obj)
                        self.session.flush()
                        
                        for genre in genres:
                            try:
                                existing_genre = self.session.query(Genres).\
                                                 filter_by(genre=genre).one()
                                genre_obj = existing_genre
                            except Exception:
                                try:
                                    genre_obj = Genres(
                                        genre=genre
                                    )
                                    self.session.add(genre_obj)
                                    self.session.flush()
                                
                                except Exception:
                                    self.session.rollback()
                            try:
                                film_genre_obj = FilmGenres(
                                    film = film_omdb_obj,
                                    genre = genre_obj
                                )
                                self.session.add(film_genre_obj)
                                self.session.flush()

                            except Exception:
                                self.session.rollback()


                        print("Collected data for {}".format(title))
                        time.sleep(0.5)
                        self.session.commit()
                    else:
                        print("WARNING! Could not find data for {}".format(title))

                except Exception as e:
                    print("WARNING! Something didn't work with {}".format(title))
                    print(e)
            
            print('Commiting {} films to database...'.format(year))
            self.commit()      
            print('\n')


if __name__ == "__main__":
    film_dict = pickle.load(open("films.p", "rb"))
    scraper = OMDB_Scraper(psql, api_key)
    scraper.scrape_omdb(film_dict)
    scraper.commit()