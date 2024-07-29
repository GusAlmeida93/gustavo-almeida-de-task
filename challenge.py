import os
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import duckdb

if __name__ == '__main__':

    API_KEY = os.environ.get('NYTIMES_API_KEY')
    API_SECRET = os.environ.get('NYTIMES_API_SECRET')


    headers = {
    "Accept": "application/json"
    }

    best_sellers = []

    best_sellers_url = f"https://api.nytimes.com/svc/books/v3/lists/full-overview.json?published_date=2021-01-01&api-key={API_KEY}"
    response_best_sellers = requests.get(best_sellers_url, headers=headers).text
    best_sellers.append(json.loads(response_best_sellers))
    next_published_date = json.loads(response_best_sellers)['results']['next_published_date']

    while '2024' not in next_published_date:
        best_sellers_url = f"https://api.nytimes.com/svc/books/v3/lists/full-overview.json?published_date={next_published_date}&api-key={API_KEY}"
        response_best_sellers = requests.get(best_sellers_url, headers=headers).text
        next_published_date = json.loads(response_best_sellers)['results']['next_published_date']
        best_sellers.append(json.loads(response_best_sellers))
        time.sleep(15)
        
        
    df = pd.DataFrame()


    for best_seller in best_sellers:
        for lst in best_seller['results']['lists']:
            df_temp = pd.DataFrame(lst['books'])
            df_temp['list_name'] = lst['list_name']
            df_temp['list_name_encoded'] = lst['list_name_encoded']
            df_temp['display_name'] = lst['display_name']
            df_temp['updated'] = lst['updated']
            df_temp['published_date'] = best_seller['results']['published_date']
            df_temp['bestsellers_date'] = best_seller['results']['bestsellers_date']
            df_temp['published_date_description'] = best_seller['results']['published_date_description']
            df_temp['previous_published_date'] = best_seller['results']['previous_published_date']
            df_temp['next_published_date'] = best_seller['results']['next_published_date']

            df = pd.concat([df, df_temp])
            

    df['created_date'] = pd.to_datetime(df['created_date'])
    df['updated_date'] = pd.to_datetime(df['updated_date'])
    df['published_date'] = pd.to_datetime(df['published_date'])
    df['bestsellers_date'] = pd.to_datetime(df['bestsellers_date'])
    df['previous_published_date'] = pd.to_datetime(df['previous_published_date'])
    df['next_published_date'] = pd.to_datetime(df['next_published_date'])


    df.to_parquet('nyt_bestsellers.parquet')

    with duckdb.connect("nyt.db") as con:
        con.sql("CREATE TABLE bestsellers AS SELECT * FROM 'nyt_bestsellers.parquet'")
        con.table("bestsellers").show()
        
        
    with duckdb.connect("nyt.db") as con:
        con.sql("""
                    SELECT
                        title
                        ,count(*) as weeks_as_top_3
                    FROM bestsellers
                    where year(published_date) = 2022
                    and rank <= 3
                    group by title
                    order by 2 desc""").show()
        con.sql("""COPY(
                        SELECT
                            title
                            ,count(*) as weeks_as_top_3
                        FROM bestsellers
                        where year(published_date) = 2022
                        and rank <= 3
                        group by title
                        order by 2 desc
                    ) TO 'top3.csv' (HEADER, DELIMITER ',')""")
        
    with duckdb.connect("nyt.db") as con:
        con.sql("""
                    SELECT
                        list_name
                        ,COUNT(DISTINCT title) as unique_titles
                    FROM bestsellers
                    GROUP BY list_name
                    ORDER BY 2 ASC
                    """).show()
        con.sql("""COPY(
                        SELECT
                            list_name
                            ,COUNT(DISTINCT title) as unique_titles
                        FROM bestsellers
                        GROUP BY list_name
                        ORDER BY 2 ASC
                    ) TO least_unique.csv (HEADER, DELIMITER ',')
                    """)
        
        
    with duckdb.connect("nyt.db") as con:
        con.sql("""with points as(
                    SELECT
                        publisher
                        ,CASE
                            WHEN rank = 1 THEN 5
                            WHEN rank = 2 THEN 4
                            WHEN rank = 3 THEN 3
                            WHEN rank = 4 THEN 2
                            WHEN rank = 5 THEN 1
                            ELSE 0
                        END AS points
                        ,published_date
                    FROM bestsellers
                    ),
                    quarter as(
                        SELECT
                            publisher
                            ,SUM(points) as points
                            ,DATE_TRUNC('quarter', published_date) as quarter_start
                            FROM points
                            GROUP BY publisher, quarter_start
                    )
                    SELECT
                        *
                        FROM quarter
                        ORDER BY quarter_start, points DESC                    
                    """).show()
        con.sql("""COPY(
                        with points as(
                            SELECT
                                publisher
                                ,CASE
                                    WHEN rank = 1 THEN 5
                                    WHEN rank = 2 THEN 4
                                    WHEN rank = 3 THEN 3
                                    WHEN rank = 4 THEN 2
                                    WHEN rank = 5 THEN 1
                                    ELSE 0
                                END AS points
                                ,published_date
                            FROM bestsellers
                            ),
                            quarter as(
                                SELECT
                                    publisher
                                    ,SUM(points) as points
                                    ,DATE_TRUNC('quarter', published_date) as quarter_start
                                    FROM points
                                    GROUP BY publisher, quarter_start
                            )
                            SELECT
                                *
                                FROM quarter
                                ORDER BY quarter_start, points DESC
                    ) TO publisher_rank.csv (HEADER, DELIMITER ',')                
                    """)
        
        
    with duckdb.connect("nyt.db") as con:
        con.sql("""with teams as(
                    SELECT
                        title
                        ,publisher
                        ,list_name
                        ,CASE
                            WHEN rank = 1 THEN 'Team Jake'
                            WHEN rank = 3 THEN 'Team Pete'
                            ELSE NULL
                        END AS teams
                        ,published_date
                    FROM bestsellers
                    )
                    SELECT
                        *
                        FROM teams
                        WHERE teams IS NOT NULL
                            AND year(published_date) = 2023        
                    """).show()
        
        con.sql("""COPY(
                        with teams as(
                        SELECT
                            title
                            ,publisher
                            ,list_name
                            ,CASE
                                WHEN rank = 1 THEN 'Team Jake'
                                WHEN rank = 3 THEN 'Team Pete'
                                ELSE NULL
                            END AS teams
                            ,published_date
                        FROM bestsellers
                        )
                        SELECT
                            *
                            FROM teams
                            WHERE teams IS NOT NULL
                                AND year(published_date) = 2023
                    ) TO pete_jake_team.csv (HEADER, DELIMITER ',')        
                    """)