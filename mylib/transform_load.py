"""
Transforms and Loads data into Databricks
"""

import os
from databricks import sql
import pandas as pd
from dotenv import load_dotenv


# Load the csv files and insert into Databricks
def load(
    dataset="data/urbanization.csv",
    dataset2="data/urbanization_state.csv",
):
    """Transforms and Loads data into Databricks"""

    # Load datasets
    print("Loading datasets...")
    df = pd.read_csv(dataset, delimiter=",")
    df2 = pd.read_csv(dataset2, delimiter=",")

    # Load environment variables for Databricks connection
    load_dotenv()
    server_host = os.getenv("SERVER_HOSTNAME")
    access_token = os.getenv("ACCESS_TOKEN")
    http_path = os.getenv("HTTP_PATH")

    try:
        # Connect to Azure Databricks
        with sql.connect(
            server_hostname=server_host,
            http_path=http_path,
            access_token=access_token,
        ) as connection:
            c = connection.cursor()

            # Check if ServeTimesDB already exists, if not, create it
            c.execute("SHOW TABLES FROM default LIKE 'urbanizationDB_tt284'")
            result = c.fetchall()

            if not result:
                # Create urbanizationDB_tt284 if it doesn't exist
                c.execute(
                    """
                    CREATE TABLE default.urbanizationDB_tt284 (
                        statefips INT, state STRING, gisjoin STRING, lat_tract FLOAT,
                        long_tract FLOAT, population INT, adj_radiuspop_5 FLOAT, urbanindex FLOAT
                    )
                    """
                )
                # Insert data into urbanizationDB_tt284
                data_to_insert_df1 = df.values.tolist()
                insert_query_df1 = """
                    INSERT INTO default.urbanizationDB_tt284 
                    (statefips, state, gisjoin, lat_tract, long_tract, population, adj_radiuspop_5, urbanindex)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """
                c.executemany(insert_query_df1, data_to_insert_df1)

            # Check if urbanization_stateDB_tt284 exists, if not, create it
            c.execute("SHOW TABLES FROM default LIKE 'urbanization_stateDB_tt284'")
            result = c.fetchall()

            if not result:
                # Create urbanization_stateDB_tt284 if it doesn't exist
                c.execute(
                    """
                    CREATE TABLE default.urbanization_stateDB_tt284 (
                        state STRING, urbanindex FLOAT
                    )
                    """
                )
                # Insert data into urbanization_stateDB_tt284
                data_to_insert_df2 = df2[["state", "urbanindex"]].values.tolist()
                insert_query_df2 = """
                    INSERT INTO default.urbanization_stateDB_tt284 (state, urbanindex)
                    VALUES (?, ?)
                """
                c.executemany(insert_query_df2, data_to_insert_df2)

            # Commit all changes
            connection.commit()

            print("Data inserted successfully.")
            return "success"

    except Exception as e:
        print(f"An error occurred: {e}")
        return "failure"


if __name__ == "__main__":
    load()
