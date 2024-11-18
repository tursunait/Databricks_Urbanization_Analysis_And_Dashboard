import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("UrbanizationDataETL").enableHiveSupport().getOrCreate()

def extract( 
    url="https://github.com/fivethirtyeight/data/raw/refs/heads/master/urbanization-index/urbanization-census-tract.csv",
    file_path="/dbfs/tmp/urbanization_census_tract.csv",
    timeout=10,
):
    """
    Downloads a file from a specified URL and saves it to the given file path.

    Args:
        url (str): The URL to download the file from.
        file_path (str): The path to save the downloaded file.
        timeout (int): Timeout for the HTTP request.

    Returns:
        str: The path to the saved file.
    """
    print(f"Downloading data from {url}...")
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()  # Raise an error for bad status codes

    with open(file_path, "wb") as file:
        file.write(response.content)

    print(f"File successfully downloaded to {file_path}")
    return file_path


def load_data(file_path):
    """
    Loads data from a CSV file into a PySpark DataFrame.

    Args:
        file_path (str): Path to the CSV file.

    Returns:
        DataFrame: Loaded Spark DataFrame.
    """
    print(f"Loading data from {file_path} into a Spark DataFrame...")
    spark_file_path = file_path.replace("/dbfs", "dbfs:")  # Adjust path for Spark
    return spark.read.csv(spark_file_path, header=True, inferSchema=True)


if __name__ == "__main__":
    # URL and paths
    dataset_url = "https://github.com/fivethirtyeight/data/raw/refs/heads/master/urbanization-index/urbanization-census-tract.csv"
    csv_file_path = "/dbfs/tmp/urbanization_census_tract.csv"

    # Step 1: Extract the dataset
    file_path = extract(url=dataset_url, file_path=csv_file_path)

    # Step 2: Load the dataset into a Spark DataFrame
    urbanization_df = load_data(file_path)

    # Step 3: Rename columns to remove special characters
    urbanization_df = urbanization_df.select(
        [col(c).alias(c.replace("(", "").replace(")", "").replace(" ", "_").replace("-", "_"))
         for c in urbanization_df.columns]
    )

    # Step 4: Show the updated schema and a few rows
    urbanization_df.printSchema()
    urbanization_df.show(5)

    # Step 5: Write the DataFrame as a table in the Databricks catalog
    print("Writing data to Databricks table...")
    urbanization_df.write.mode("overwrite").saveAsTable("ids706_data_engineering.default.urbanization_census_tract")
    print("Data successfully written to the Databricks table.")
