import sqlite3
import requests
import pandas as pd


def create_tables():
    conn = sqlite3.connect('javatpoint.db')

    conn.execute("""CREATE TABLE flights
           (ID INT PRIMARY KEY     NOT NULL,
           CHLOCCT           TEXT    NOT NULL,
           CHRMINE            TEXT     NOT NULL);""")
    conn.commit()

    conn.close()


def get_flights():
    conn = sqlite3.connect('javatpoint.db')

    URL = r"https://data.gov.il/api/3/action/datastore_search?resource_id=e83f763b-b7d7-479e-b172-ae981ddc6de5&limit=1000"
    r = requests.get(url=URL)

    data = r.json()
    print(data['result']['records'][0])
    for record in data['result']['records']:
        conn.execute(f"INSERT INTO flights (ID,CHLOCCT,CHRMINE) \
        VALUES ({record['_id']}, '{record['CHLOCCT']}', '{record['CHRMINE']}' )")
        conn.commit()

    conn.close()


def get_countries():
    conn = sqlite3.connect('javatpoint.db')

    URL = r"https://www.geonames.org/countries/"
    r = requests.get(url=URL)
    df_1 = pd.read_html(r.text)
    df_1[1].to_sql('countries', conn, if_exists='replace', index=False)

    conn.close()


def first_q():
    conn = sqlite3.connect('javatpoint.db')

    data = conn.execute("""select CHLOCCT, count(1) from flights
                        where CHRMINE = 'DELAYED'
                         group by CHLOCCT
                         order by 2 desc
                         limit 3""")

    for row in data:
        print(row)

    conn.close()


def second_q():
    conn = sqlite3.connect('javatpoint.db')

    data = conn.execute("""select f.* from countries c
    join flights f on UPPER(f.CHLOCCT) = UPPER(c.Country)
    where f.CHRMINE = 'LANDED'
    and c.'Area in km²' > 1000000""")

    for row in data:
        print(row)

    conn.close()


def third_q():
    conn = sqlite3.connect('javatpoint.db')

    data = conn.execute("""
    SELECT
          country,
          cnt,
          -(LEAD(cnt, 1, 0) OVER (ORDER BY cnt desc) - cnt)
    FROM
    (select c.Country as country, count(1) as cnt from countries c
    join flights f on UPPER(f.CHLOCCT) = UPPER(c.Country)
    where  c.Population > 10000000
    group by f.CHLOCCT
    having count(1) < 50
    order by 2 desc)
    """)

    for row in data:
        print(row)

    conn.close()


if __name__ == '__main__':
    # create_tables()
    # get_flights()
    get_countries()
    first_q()
    second_q()
    third_q()
