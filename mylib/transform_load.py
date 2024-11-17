"""
Transforms and Loads data into Databricks
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col


def clean_columns(df):
    """
    Cleans column names to remove special characters and spaces.

    Args:
        df (DataFrame): Spark DataFrame to clean.

    Returns:
        DataFrame: DataFrame with cleaned column names.
    """
    print("Cleaning column names...")
    return df.select(
        [
            col(c).alias(
                c.replace("(", "")
                .replace(")", "")
                .replace(" ", "_")
                .replace("-", "_")
                .replace("/", "_")
            )
            for c in df.columns
        ]
    )


def load_data(
    dataset_path="dbfs:/tmp/urbanization_census_tract.csv",
    database="default",
    table="urbanization_data_tt284",
):
    """
    Transforms and Loads data into Databricks using Spark.

    Args:
        dataset_path (str): Path to the dataset file.
        database (str): Database name in Databricks.
        table (str): Table name to write data.

    Returns:
        str: Success or failure message.
    """
    # Initialize Spark session
    spark = SparkSession.builder.appName("UrbanizationDataLoad").getOrCreate()

    try:
        # Load dataset into a Spark DataFrame
        print(f"Loading dataset from {dataset_path} into Spark DataFrame...")
        df = spark.read.csv(dataset_path, header=True, inferSchema=True)

        # Clean column names
        df = clean_columns(df)

        # Ensure numeric column types where applicable (example: "lat_tract")
        if "lat_tract" in df.columns:
            df = df.withColumn("lat_tract", df["lat_tract"].cast(DoubleType()))

        # Write the DataFrame into a Databricks table
        print(f"Writing data to table: {database}.{table}")
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{database}.{table}")

        print(f"Data successfully written to {database}.{table}")
        return "success"

    except Exception as e:
        print(f"An error occurred: {e}")
        return "failure"


if __name__ == "__main__":
    load_data()
