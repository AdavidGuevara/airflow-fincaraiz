from playwright.sync_api import sync_playwright
from lxml import etree
import pandas as pd
import sqlalchemy
import requests
import sqlite3
import random
import time


def db_raw(city: str):
    conn = sqlite3.connect(f"houses.db")
    cursor = conn.cursor()

    sql_query = f"""
    CREATE TABLE IF NOT EXISTS {city}_raw(
        id_house TEXT,
        region TEXT,
        area TEXT,
        rooms TEXT,
        bathrooms TEXT,
        parking TEXT,
        stratum TEXT,
        price TEXT
    )
    """
    cursor.execute(sql_query)


def extract_data(number_page: int, city: str, state: str):
    conn = sqlite3.connect(f"houses.db")
    cursor = conn.cursor()

    USERAGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_4_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36 Edg/87.0.664.75",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.18363",
    ]

    # Extract the url of each house and its price:
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(user_agent=random.choice(USERAGENTS))
        index_page = context.new_page()

        index_page.goto(
            url=f"https://www.fincaraiz.com.co/apartamentos/arriendos/{city}/{state}?pagina={number_page}",
            wait_until="domcontentloaded",
        )
        index_page.wait_for_selector(".MuiPaper-root")

        url_parsed = []
        urls = index_page.locator("//article[contains(@class, 'MuiCard-root')]")
        for url in urls.element_handles():
            url_parsed.append(
                {
                    "url": "https://www.fincaraiz.com.co"
                    + url.query_selector("a.MuiTypography-root").get_attribute("href"),
                    "price": url.query_selector(
                        "span.MuiTypography-root b"
                    ).text_content(),
                }
            )

        context.close()
        browser.close()

    # Extracting raw data from the house and saving it to the dataframe:
    houses = []
    for item in url_parsed:
        try:
            r = requests.get(
                item["url"], headers={"User-agent": random.choice(USERAGENTS)}
            )

            page = r.text

            path = "//div[contains(@class, 'jss252')]"
            if etree.HTML(page).xpath(path):
                if len(etree.HTML(page).xpath(path + "/div")) == 2:
                    path = path + "/div[1]"

                region = (
                    etree.HTML(page)
                    .xpath("//header/div[contains(@class, 'MuiBox-root')]/p[2]")[0]
                    .text
                )
                rooms = etree.HTML(page).xpath(path + "/div[1]/div[2]/p[2]")[0].text
                bathrooms = etree.HTML(page).xpath(path + "/div[2]/div[2]/p[2]")[0].text

                parking = False
                if (
                    etree.HTML(page).xpath(path + "/div[3]/div[2]/p[1]")[0].text
                    == "Parqueaderos"
                ):
                    parking = True

                area = etree.HTML(page).xpath(path + "/div[3]/div[2]/p[2]")[0].text
                if (
                    etree.HTML(page).xpath(path + "/div[4]/div[2]/p[1]")[0].text
                    == "Área construída"
                ):
                    area = etree.HTML(page).xpath(path + "/div[4]/div[2]/p[2]")[0].text

                stratum = etree.HTML(page).xpath(path + "/div[5]/div[2]/p[2]")[0].text
                if (
                    etree.HTML(page).xpath(path + "/div[6]/div[2]/p[1]")[0].text
                    == "Estrato"
                ):
                    stratum = (
                        etree.HTML(page).xpath(path + "/div[6]/div[2]/p[2]")[0].text
                    )

                cursor.execute(
                    f"""
                    INSERT INTO {city}_raw (id_house, region, area, rooms, bathrooms, parking, stratum, price) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        item["url"],
                        region,
                        area,
                        rooms,
                        bathrooms,
                        parking,
                        stratum,
                        item["price"],
                    ),
                )
                conn.commit()
            time.sleep(0.25)
        except:
            url = item["url"]
            raise Exception(f"Page {url} not Completed.")


def clean_data(city: str):
    engine = sqlalchemy.create_engine(f"sqlite:///houses.db")
    conn = sqlite3.connect(f"houses.db")
    cursor = conn.cursor()

    raw_data_df = pd.read_sql(f"SELECT * FROM {city}_raw", conn)

    # clean and transformation data:
    for i in range(0, raw_data_df.shape[0]):
        raw_data_df.loc[i, "id_house"] = raw_data_df.loc[i, "id_house"].split("/")[-1]
        raw_data_df.loc[i, "region"] = raw_data_df.loc[i, "region"].split(" - ")[0]
        raw_data_df.loc[i, "area"] = raw_data_df.loc[i, "area"].replace(" m²", "")
        raw_data_df.loc[i, "price"] = (
            raw_data_df.loc[i, "price"].replace("$", "").replace(".", "")
        )

    raw_data_df["area"] = raw_data_df["area"].astype("int")
    raw_data_df["rooms"] = raw_data_df["rooms"].astype("int")
    raw_data_df["bathrooms"] = raw_data_df["bathrooms"].astype("int")
    raw_data_df["stratum"] = raw_data_df["stratum"].astype("int")
    raw_data_df["area"] = raw_data_df["area"].astype("int")

    # Load clean data:
    sql_query = f"""
    CREATE TABLE IF NOT EXISTS {city}_clean(
        id INTEGER,
        id_house TEXT,
        region TEXT,
        area INTEGER,
        rooms INTEGER,
        bathrooms INTEGER,
        parking BOOLEAN,
        stratum INTEGER,
        price INTEGER,
        CONSTRAINT primary_key_constraint PRIMARY KEY (id)
    )
    """
    cursor.execute(sql_query)
    raw_data_df.to_sql(f"{city}_clean", engine, index=False, if_exists="append")
