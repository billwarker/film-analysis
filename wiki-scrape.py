from bs4 import BeautifulSoup
import requests
import pickle

film_dict = {}

for year in range(2018, 2009, -1):
    url = "https://en.wikipedia.org/wiki/{}_in_film".format(year)
    r = requests.get(url)
    html = BeautifulSoup(r.text, "lxml")
    print(url)

    films = []

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
                        film = row_data[1].text.strip()
                        films.append(film)
                    elif len(row_data) == n_cols - 1:
                        film = row_data[0].text.strip()
                        films.append(film)
                except Exception as e:
                    print(e)
        
    print("{} had {} films.".format(year, len(films)))
    print(films)
    film_dict[year] = films

pickle.dump(film_dict, open("films.p", "wb"))