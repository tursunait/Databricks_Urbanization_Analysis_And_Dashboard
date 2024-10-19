"""
Transforms and Loads data into the Databricks database
"""

import os
from databricks import sql
import pandas as pd
from dotenv import load_dotenv


def load(
    dataset="data/urbanization.csv",
    dataset2="data/urbanization_state.csv",
):
    """Transforms and Loads the top 5 locations by urbanindex for each state into the Databricks database."""
    print("Loading datasets...")
    df = pd.read_csv(dataset, delimiter=",")
    df2 = pd.read_csv(dataset2, delimiter=",")

    # Transform: Select top 5 locations by urbanindex for each state in df
    df_top5 = df.sort_values(by=["state", "urbanindex"], ascending=[True, False])
    df_top5 = df_top5.groupby("state").head(5).reset_index(drop=True)

    # Load environment variables for Databricks connection
    load_dotenv()
    server_host = os.getenv("SERVER_HOSTNAME")
    access_token = os.getenv("ACCESS_TOKEN")
    http_path = os.getenv("HTTP_PATH")

    try:
        # Connect to Databricks
        with sql.connect(
            server_hostname=server_host,
            http_path=http_path,
            access_token=access_token,
        ) as connection:
            c = connection.cursor()

            # Drop the existing table and create a new one for urbanizationDB
            c.execute("DROP TABLE IF EXISTS default.urbanizationDB_tt284")
            c.execute(
                """
                CREATE TABLE default.urbanizationDB_tt284 (
                    statefips INT, state STRING, gisjoin STRING, lat_tract FLOAT,
                    long_tract FLOAT, population INT, adj_radiuspop_5 FLOAT, urbanindex FLOAT
                )
                """
            )

            # Prepare the transformed data (top 5 urban index locations) for insertion
            data_to_insert_df1 = df_top5.values.tolist()
            insert_query_df1 = """
                INSERT INTO default.urbanizationDB_tt284 
                (statefips, state, gisjoin, lat_tract, long_tract, population, adj_radiuspop_5, urbanindex)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            c.executemany(insert_query_df1, data_to_insert_df1)

            # Commit the changes after inserting into the first table
            connection.commit()

            # Now create the second table for df2 (urbanization_stateDB_tt284)
            c.execute("DROP TABLE IF EXISTS default.urbanization_stateDB_tt284")
            c.execute(
                """
                CREATE TABLE default.urbanization_stateDB_tt284 (
                    state STRING, urbanindex_state FLOAT
                )
                """
            )

            # Prepare the data for insertion from df2
            data_to_insert_df2 = df2[["state", "urbanindex"]].values.tolist()
            insert_query_df2 = """
                INSERT INTO default.urbanization_stateDB_tt284 (state, urbanindex)
                VALUES (?, ?)
            """
            c.executemany(insert_query_df2, data_to_insert_df2)

            # Commit the changes after inserting into the second table
            connection.commit()

            c.close()  # Ensure the cursor is closed after operations

        print("Data inserted successfully.")
        return "success"

    except Exception as e:
        print(f"An error occurred: {e}")
        return "failure"


if __name__ == "__main__":
    load()
