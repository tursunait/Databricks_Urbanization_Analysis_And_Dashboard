"""
Extract a dataset 
urbanization dataset
"""
from pyspark.sql import SparkSession
import requests
import os

def extract(
    url="https://github.com/fivethirtyeight/data/raw/refs/heads/master/urbanization-index/urbanization-census-tract.csv",
    url2="https://github.com/fivethirtyeight/data/raw/refs/heads/master/urbanization-index/urbanization-state.csv",
    file_path="/dbfs/tmp/urbanization.csv",
    file_path2="/dbfs/tmp/urbanization_state.csv",
    directory="/dbfs/tmp",
):
    """Extract a url to a Databricks DBFS path"""
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Download and save the first file
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)

    # Download and save the second file
    with requests.get(url2) as r:
        with open(file_path2, "wb") as f:
            f.write(r.content)

    # Initialize Spark session
    spark = SparkSession.builder.appName("UrbanizationDataExtraction").getOrCreate()

    # Read the second file into a Spark DataFrame
    df = spark.read.csv(file_path2, header=True, inferSchema=True)

    # Select the first 121 rows
    df_subset = df.limit(121)

    # Write the subset back to the file
    df_subset.write.csv(file_path2, mode="overwrite", header=True)

    return file_path, file_path2


if __name__ == "__main__":
    extract()

