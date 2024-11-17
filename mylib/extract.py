"""
Extract a dataset 
Urbanization dataset
"""
import os
import requests
from pyspark.sql import SparkSession
from dotenv import load_dotenv
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


def is_databricks_environment():
    """Check if the script is running in Databricks."""
    return os.getenv("DATABRICKS_RUNTIME_VERSION") is not None


def convert_path(path):
    """Convert file paths based on the environment."""
    if is_databricks_environment():
        return path
    else:
        # Convert `dbfs:/` paths to local paths for non-Databricks environments
        return path.replace("dbfs:/", "/tmp/")


def ensure_directory_exists(path):
    """Ensures that the directory for the given path exists."""
    directory = os.path.dirname(path)
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")
    else:
        print(f"Directory already exists: {directory}")


def extract(
    url="https://github.com/fivethirtyeight/data/raw/refs/heads/master/urbanization-index/urbanization-census-tract.csv",
    url2="https://github.com/fivethirtyeight/data/raw/refs/heads/master/urbanization-index/urbanization-state.csv",
    file_path="dbfs:/tmp/urbanization.csv",
    file_path2="dbfs:/tmp/urbanization_state.csv",
):
    """Extract URLs to Databricks DBFS paths and process with Spark."""
    # Load environment variables
    load_dotenv()
    server_host = os.getenv("SERVER_HOSTNAME")
    access_token = os.getenv("ACCESS_TOKEN")
    http_path = os.getenv("HTTP_PATH")

    if not server_host or not access_token or not http_path:
        raise ValueError("Environment variables SERVER_HOSTNAME, ACCESS_TOKEN, or HTTP_PATH are missing.")

    print(f"Using server: {server_host}, HTTP Path: {http_path}")

    # Convert paths for local environment
    file_path = convert_path(file_path)
    file_path2 = convert_path(file_path2)
    conflicting_path = convert_path("dbfs:/tmp/urbanization_state_subset/")

    # Ensure directories exist for local paths
    ensure_directory_exists(file_path)
    ensure_directory_exists(file_path2)

    # Remove conflicting directory
    try:
        if is_databricks_environment():
            dbutils.fs.rm(conflicting_path, recurse=True)
        else:
            if os.path.exists(conflicting_path):
                os.rmdir(conflicting_path)
        print(f"Removed conflicting directory: {conflicting_path}")
    except Exception as e:
        print(f"Could not remove conflicting path: {conflicting_path}. Error: {e}")

    # Download and save files
    print("Downloading and saving files...")
    try:
        data1 = requests.get(url).content.decode("utf-8")
        data2 = requests.get(url2).content.decode("utf-8")

        if is_databricks_environment():
            print("Saving to DBFS...")
            dbutils.fs.put(file_path, data1, overwrite=True)
            dbutils.fs.put(file_path2, data2, overwrite=True)
            print("Files saved to DBFS.")
        else:
            print("Saving to local filesystem...")
            with open(file_path, "w") as f:
                f.write(data1)
            with open(file_path2, "w") as f:
                f.write(data2)
            print(f"Files saved locally: {file_path}, {file_path2}")
    except Exception as e:
        print(f"Error writing files: {e}")
        return

    # Initialize Spark session
    spark = SparkSession.builder.appName("UrbanizationDataExtraction").getOrCreate()

    # Read the second file into a Spark DataFrame
    try:
        df = spark.read.csv(file_path2, header=True, inferSchema=True)
    except Exception as e:
        print(f"Error reading the file into Spark DataFrame: {e}")
        return

    # Select the first 121 rows
    df_subset = df.limit(121)

    # Save the subset to a unique directory
    unique_output_dir = convert_path("dbfs:/tmp/urbanization_state_subset/")
    try:
        df_subset.coalesce(1).write.mode("overwrite").csv(unique_output_dir, header=True)
    except Exception as e:
        print(f"Error saving the subset: {e}")
        return

    # Retrieve the exact file path
    try:
        if is_databricks_environment():
            output_files = dbutils.fs.ls(unique_output_dir)
            output_file = [
                f"{unique_output_dir}{file_info.name}"
                for file_info in output_files
                if file_info.name.endswith(".csv")
            ][0]
        else:
            output_files = os.listdir(unique_output_dir)
            output_file = [
                os.path.join(unique_output_dir, f) for f in output_files if f.endswith(".csv")
            ][0]

        print(f"Subset saved to {output_file}")
    except Exception as e:
        print(f"Error retrieving the exact file path: {e}")


if __name__ == "__main__":
    extract()