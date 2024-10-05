"""
Transforms and Loads data into the local SQLite3 database

"""

import sqlite3
import csv
import os


# load the csv file and insert into a new sqlite3 database
def load(
    dataset="/Users/tusunaiturumbekova/Desktop/DE/py_script_with_SQLDatabase/data/urbanization.csv",
):
    """ "Transforms and Loads data into the local SQLite3 database"""

    # prints the full working directory and path
    print(os.getcwd())
    payload = csv.reader(open(dataset, newline=""), delimiter=",")
    conn = sqlite3.connect("urbanizationDB.db")
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS urbanizationDB")
    c.execute(
        "CREATE TABLE urbanizationDB (statefips,state,gisjoin,lat_tract,long_tract,population,adj_radiuspop_5,urbanindex)"
    )
    # insert
    c.executemany("INSERT INTO urbanizationDB VALUES (?,?, ?, ?, ?, ?, ?, ?)", payload)
    conn.commit()
    conn.close()
    return "urbanizationDB"


if __name__ == "__main__":
    load()
