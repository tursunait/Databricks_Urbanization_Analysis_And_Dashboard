"""
Transforms and Loads data into the local SQLite3 database

"""

import os
from databricks import sql
import pandas as pd
from dotenv import load_dotenv


# load the csv file and insert into databricks
def load(
    dataset="/Users/tusunaiturumbekova/Desktop/DE/sql_query_databricks1_tursunaiv/data/urbanization.csv",
):
    """Transforms and Loads data into the local databricks database"""
    df = pd.read_csv(dataset, delimiter=",")
    load_dotenv()
    server_host = os.getenv("SERVER_HOSTNAME")
    access_token = os.getenv("ACCESS_TOKEN")
    http_path = os.getenv("HTTP_PATH")
    with sql.connect(
        server_hostname=server_host,
        http_path=http_path,
        access_token=access_token,
    ) as connection:
        c = connection.cursor()
        c.execute("SHOW TABLES FROM default LIKE 'urbanization*'")
        result = c.fetchall()
        c.execute("DROP TABLE IF EXISTS urbanizationDB")
        c.execute(
            "CREATE TABLE urbanizationDB (statefips,state,gisjoin,lat_tract,long_tract,population,adj_radiuspop_5,urbanindex)"
        )
        # insert
        c.executemany("INSERT INTO urbanizationDB VALUES (?,?, ?, ?, ?, ?, ?, ?)")
        c.close()

    return "success"
