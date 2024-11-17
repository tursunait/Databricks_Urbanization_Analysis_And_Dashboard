"""
Transforms and Loads data into Databricks
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from dotenv import load_dotenv


def load(
    dataset="/dbfs/tmp/urbanization.csv",
    dataset2="/dbfs/tmp/urbanization_state.csv",
):
    """Transforms and Loads data into Databricks using Spark"""

    # Initialize Spark session
    spark = SparkSession.builder.appName("UrbanizationDataLoad").getOrCreate()

    # Load datasets into Spark DataFrames
    print("Loading datasets into Spark DataFrames...")
    df = spark.read.csv(dataset, header=True, inferSchema=True)
    df2 = spark.read.csv(dataset2, header=True, inferSchema=True)

    # Standardize column names
    df = df.toDF(*[col.lower() for col in df.columns])
    df2 = df2.toDF(*[col.lower() for col in df2.columns])

    # Cast lat_tract to the correct data type
    df = df.withColumn("lat_tract", df["lat_tract"].cast(DoubleType()))

    # Load environment variables for Databricks connection
    load_dotenv()
    database = "default"
    table1 = "urbanizationdb_tt284"
    table2 = "urbanization_stateDB_tt284"

    try:
        # Use Spark to create or replace the first table and insert data
        print(f"Writing data to table: {database}.{table1}")
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{database}.{table1}")

        # Use Spark to create or replace the second table and insert data
        print(f"Writing data to table: {database}.{table2}")
        df2.select("state", "urbanindex").write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{database}.{table2}")

        print("Data inserted successfully.")
        return "success"

    except Exception as e:
        print(f"An error occurred: {e}")
        return "failure"


if __name__ == "__main__":
    load()
