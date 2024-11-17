"""
Extract a dataset 
urbanization dataset
"""
from pyspark.sql import SparkSession
import requests

# Fallback to mock dbutils if running outside of Databricks
try:
    from pyspark.dbutils import DBUtils
    spark = SparkSession.builder.appName("UrbanizationDataExtraction").getOrCreate()
    dbutils = DBUtils(spark)
except ImportError:
    print("dbutils is not available in this environment. Using a mock implementation.")
    class MockDBUtils:
        def fs(self):
            return self

        def rm(self, path, recurse):
            print(f"Mock remove: {path}, recurse={recurse}")

        def put(self, path, data, overwrite):
            print(f"Mock put: {path}, overwrite={overwrite}")

        def ls(self, path):
            print(f"Mock ls: {path}")
            return []

    dbutils = MockDBUtils()


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
    data1 = requests.get(url).content.decode("utf-8")
    data2 = requests.get(url2).content.decode("utf-8")

    try:
        dbutils.fs.put(file_path, data1, overwrite=True)
        dbutils.fs.put(file_path2, data2, overwrite=True)
        print(f"Files written to {file_path} and {file_path2}")
    except Exception as e:
        print(f"Error writing files: {e}")
        raise

    # Initialize Spark session
    spark = SparkSession.builder.appName("UrbanizationDataExtraction").getOrCreate()

    # Read the second file into a Spark DataFrame
    df = spark.read.csv(file_path2, header=True, inferSchema=True)

    # Select the first 121 rows
    df_subset = df.limit(121)

    # Save the subset to a unique directory
    unique_output_dir = "dbfs:/tmp/urbanization_state_subset/"
    df_subset.coalesce(1).write.mode("overwrite").csv(unique_output_dir, header=True)

    # Verify the written files
    try:
        output_files = dbutils.fs.ls(unique_output_dir)
        output_file = [f.path for f in output_files if f.path.endswith(".csv")][0]
        print(f"Subset saved to {output_file}")
    except Exception as e:
        print(f"Failed to locate files in {unique_output_dir}: {e}")
        raise


if __name__ == "__main__":
    extract()
