import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3

url = 'https://www.sephora.sg'
r = requests.get(url)

soup = BeautifulSoup(r.content, 'html5lib')
table = soup.find('div', attrs = {"class":"column brands-search"})

brands = []

for row in table.find_all_next("li", attrs = {"class":"menu-item"}):
    brand = {}
    brand["name"] = row.a.get("data-analytics-label")
    brand['href'] = row.a['href']
    if brand["name"] != None:
        brands.append(brand)

conn = sqlite3.connect("sephora.db")

brands = pd.DataFrame(brands)

brands.to_sql("brands", conn, if_exists='replace')