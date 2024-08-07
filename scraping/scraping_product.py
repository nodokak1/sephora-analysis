import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3

conn = sqlite3.connect("sephora.db")

url = 'https://www.sephora.sg'

r = requests.get(url)
soup = BeautifulSoup(r.content, 'html5lib')

brands_df = pd.read_sql("SELECT * FROM brands", conn)

products = []

for _, brand in brands_df.iterrows():
    url_brand = url + brand['href']
    r_two = requests.get(url_brand)
    soup_two = BeautifulSoup(r_two.content, 'html5lib')
    table = soup_two.find('div', attrs = {"class":"products-grid-container"})

    if table is None:
        continue

    for row in table.find_all_next("div", attrs = {"class":"products-card-container"}):
        product = {}
        product['product_id'] = row.div.get("data-product-id")
        product["brand"] = brand['name']
        product["name"] = row.div.get("data-product-name")
        product['href'] = row.a['href']
        products.append(product)

products = pd.DataFrame(products)
products.to_sql("products", conn, if_exists='replace')