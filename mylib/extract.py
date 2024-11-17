import requests
from pyspark.sql import SparkSession


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
    response.raise_for_status()

    with open(file_path, "wb") as file:
        file.write(response.content)

    print(f"File successfully downloaded to {file_path}")
    return file_path


def load_csv(file_path):
    """
    Loads data from a CSV file into a PySpark DataFrame.

    Args:
        file_path (str): Path to the CSV file.

    Returns:
        DataFrame: Loaded Spark DataFrame.
    """
    print(f"Loading data from {file_path}...")
    # Remove the /dbfs prefix for Spark to properly access the file in Databricks
    spark_file_path = file_path.replace("/dbfs", "dbfs:")
    spark = SparkSession.builder.appName("UrbanizationDataETL").getOrCreate()
    return spark.read.csv(spark_file_path, header=True, inferSchema=True)


def save_random_sample(df, output_path, sample_fraction=0.1, seed=42):
    """
    Saves a random sample of the DataFrame as a CSV.

    Args:
        df (DataFrame): DataFrame to sample.
        output_path (str): Output path for the CSV file.
        sample_fraction (float): Fraction of the DataFrame to sample (0 < fraction <= 1).
        seed (int): Seed for random sampling.

    Returns:
        str: Path to the saved file.
    """
    print(f"Saving a random sample (fraction={sample_fraction}) to {output_path}...")
    sampled_df = df.sample(withReplacement=False, fraction=sample_fraction, seed=seed)
    sampled_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
    print(f"Random sample saved to {output_path}")
    return output_path


if __name__ == "__main__":
    # URL and paths
    dataset_url = "https://github.com/fivethirtyeight/data/raw/refs/heads/master/urbanization-index/urbanization-census-tract.csv"
    csv_file_path = "/dbfs/tmp/urbanization_census_tract.csv"
    output_dir = "dbfs:/tmp/urbanization_census_subset/"

    # Step 1: Extract the dataset
    file_path = extract(url=dataset_url, file_path=csv_file_path)

    # Step 2: Load the dataset into a Spark DataFrame
    df = load_csv(file_path)

    # Step 3: Save a random sample of the DataFrame
    save_random_sample(df, output_dir, sample_fraction=0.1)

    print("ETL process with random sampling completed successfully.")
