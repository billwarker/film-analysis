import bs4
import requests
import re
import datetime
from models import HolidaysUSA

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (scoped_session, sessionmaker)
from settings import psql

f = open("holidays.txt")

pattern = re.compile(r"([A-Za-z]+ [0-9]+)\t([\w() '\.]+)")

holiday_dict = {} 

for line in f.readlines():
    text = line.replace('\n','')
    try:
        year = int(text)
    except ValueError:
        holiday_str = re.match(pattern, text)
        try:
            date = holiday_str.group(1)
            month, day = date.split(' ')
            if len(day) == 1: day = "0" + day
            full_date = "{} {} {}".format(month, day, year)
            date_obj = datetime.datetime.strptime(full_date, "%b %d %Y")
            holiday = holiday_str.group(2)
            holiday_dict[date_obj] = holiday
        except AttributeError:
            pass



engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
						psql['user'], psql['password'],
						psql['host'], psql['port'],
						psql['database']))

Base = declarative_base()
Base.metadata.bind = engine

engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
                            psql['user'], psql['password'],
                            psql['host'], psql['port'],
                            psql['database']))

Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()

for date in list(holiday_dict.keys()):
    holiday_obj = HolidaysUSA(
        date = date,
        holiday = holiday_dict[date]
    )
    session.add(holiday_obj)
    print("Added {}".format(holiday_obj.holiday))

session.commit()