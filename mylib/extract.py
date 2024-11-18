import requests
from pyspark.sql.functions import col

def extract( 
    url="https://github.com/fivethirtyeight/data/raw/refs/heads/master/urbanization-index/urbanization-census-tract.csv",
    file_path="/dbfs/tmp/urbanization_census_tract.csv",  # Save the file temporarily in DBFS
    timeout=10,
):
    """
    Downloads a file from a specified URL and saves it to the given file path.
    """
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()  # Raise an error for bad status codes
    with open(file_path, "wb") as file:
        file.write(response.content)
    return file_path


def load_data(file_path):
    """
    Loads data from a CSV file into a PySpark DataFrame.
    """
    return spark.read.csv("dbfs:/tmp/urbanization_census_tract.csv", header=True, inferSchema=True)


# Extract the dataset
file_path = extract()

# Load it into a Spark DataFrame
urbanization_df= load_data(file_path)

# Rename columns to remove special characters
urbanization_df = urbanization_df.select(
    [col(c).alias(c.replace("(", "").replace(")", "").replace(" ", "_").replace("-", "_")) for c in urbanization_df.columns]
)

# Show the updated schema and a few rows
urbanization_df.printSchema()
urbanization_df.show(5)

# Write the DataFrame as a table in the Databricks catalog
urbanization_df.write.mode("overwrite").saveAsTable("ids706_data_engineering.default.urbanization_census_tract")
