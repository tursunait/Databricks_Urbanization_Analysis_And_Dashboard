"""
Extract a dataset 
urbanization dataset
"""
import requests
from pyspark.sql import SparkSession
from unittest.mock import MagicMock

# Mock dbutils if not in Databricks
try:
    from pyspark.dbutils import DBUtils
    spark = SparkSession.builder.appName("UrbanizationDataExtraction").getOrCreate()
    dbutils = DBUtils(spark)
except ImportError:
    print("dbutils is not available in this environment. Using a mock implementation.")
    dbutils = MagicMock()
    dbutils.fs.rm = MagicMock()
    dbutils.fs.put = MagicMock()
    dbutils.fs.ls = MagicMock(return_value=[])


def extract(
    url="https://github.com/fivethirtyeight/data/raw/refs/heads/master/urbanization-index/urbanization-census-tract.csv",
    url2="https://github.com/fivethirtyeight/data/raw/refs/heads/master/urbanization-index/urbanization-state.csv",
    file_path="dbfs:/tmp/urbanization.csv",
    file_path2="dbfs:/tmp/urbanization_state.csv",
):
    """Extract URLs to Databricks DBFS paths and process with Spark."""
    # Remove conflicting directory
    conflicting_path = "dbfs:/tmp/urbanization_state_subset/"
    try:
        dbutils.fs.rm(conflicting_path, recurse=True)
        print(f"Removed conflicting directory: {conflicting_path}")
    except Exception as e:
        print(f"Could not remove conflicting path: {conflicting_path}. Error: {e}")

    # Download and save files
    print("Downloading and saving files...")
    try:
        data1 = requests.get(url).content.decode("utf-8")
        data2 = requests.get(url2).content.decode("utf-8")

        dbutils.fs.put(file_path, data1, overwrite=True)
        dbutils.fs.put(file_path2, data2, overwrite=True)
    except Exception as e:
        print(f"Error writing files: {e}")
        return

    # Initialize Spark session
    spark = SparkSession.builder.appName("UrbanizationDataExtraction").getOrCreate()

    # Read the second file into a Spark DataFrame
    df = spark.read.csv(file_path2, header=True, inferSchema=True)

    # Select the first 121 rows
    df_subset = df.limit(121)

    # Save the subset to a unique directory
    unique_output_dir = "dbfs:/tmp/urbanization_state_subset/"
    df_subset.coalesce(1).write.mode("overwrite").csv(unique_output_dir, header=True)

    # Retrieve the exact file path
    output_files = dbutils.fs.ls(unique_output_dir)
    output_file = [f.path for f in output_files if f.path.endswith(".csv")][0]
    print(f"Subset saved to {output_file}")


if __name__ == "__main__":
    extract()
