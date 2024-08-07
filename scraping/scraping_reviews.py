import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_reviews(product, url):
    url_product = url + product['href']
    r_three = requests.get(url_product)

    soup_three = BeautifulSoup(r_three.content, 'html5lib')
    table = soup_three.find('div', attrs={"class": "product-reviews-container"})

    if table is None:
        return []

    reviews = []
    for row in table.find_all_next("div", attrs={"class": "row rating-bar"}):
        review = {}
        review['product_id'] = product['product_id']
        review["brand"] = product['brand']
        review["name"] = product['name']
        review['stars'] = row.find('div', class_='col star-text').text.strip()
        review['num'] = row.find('div', class_='bar-value').text.strip()
        reviews.append(review)
        print(review)
    return reviews

def main():
    conn = sqlite3.connect("sephora.db")
    products_df = pd.read_sql("SELECT * FROM products", conn)
    url = 'https://www.sephora.sg'

    reviews = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_product = {executor.submit(fetch_reviews, product, url): product for _, product in products_df.iterrows()}
        for future in as_completed(future_to_product):
            product_reviews = future.result()
            reviews.extend(product_reviews)
            for review in product_reviews:
                print(review)

    # Optionally, save reviews to the database or a file here

if __name__ == "__main__":
    main()