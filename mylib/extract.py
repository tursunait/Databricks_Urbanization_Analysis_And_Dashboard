"""
Extract a dataset 
urbanization dataset
"""
try:
    from pyspark.dbutils import DBUtils  # Import dbutils in Databricks
except ImportError:
    from mocks import dbutils  # Use mock for local testing

from pyspark.sql import SparkSession
import requests


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
    except requests.exceptions.RequestException as e:
        print(f"Failed to download files: {e}")
        return

    try:
        dbutils.fs.put(file_path, data1, overwrite=True)
        dbutils.fs.put(file_path2, data2, overwrite=True)
        print("Files saved to DBFS.")
    except Exception as e:
        print(f"Failed to save files to DBFS: {e}")
        return

    # Initialize Spark session
    spark = SparkSession.builder.appName("UrbanizationDataExtraction").getOrCreate()

    try:
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
    except Exception as e:
        print(f"Failed to process and save subset: {e}")


if __name__ == "__main__":
    extract()
