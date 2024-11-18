'''
    Transform-Load
'''
from pyspark.sql.functions import col, year, month, dayofmonth

def transform(data_path="data/urbanization_census_tract.csv", spark=None):
    if spark is None:
        raise ValueError("A Spark session must be provided.")

    df = spark.read.csv(data_path, header=True, inferSchema=True)

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

def load(df, output_path="output/urbanization_census_tract.parquet", file_format="parquet"):
    if file_format == "parquet":
        df.write.mode("overwrite").parquet(output_path)
    elif file_format == "csv":
        df.write.mode("overwrite").option("header", True).csv(output_path)
    elif file_format == "json":
        df.write.mode("overwrite").json(output_path)
    else:
        raise ValueError("Unsupported file format. Choose 'parquet', 'csv', or 'json'.")

    return f"Data saved to {output_path} in {file_format} format."