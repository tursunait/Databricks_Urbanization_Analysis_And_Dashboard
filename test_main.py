import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from main import main

class TestUrbanizationDataPipeline(unittest.TestCase):

    @patch("main.extract")
    @patch("main.transform")
    @patch("main.load")
    @patch("main.query_table")
    @patch("main.visualize_population_vs_urbanindex")
    @patch("main.visualize_urbanindex_distribution")
    def test_main_pipeline(
        self,
        mock_visualize_distribution,
        mock_visualize_population,
        mock_query_table,
        mock_load,
        mock_transform,
        mock_extract,
    ):
        # Mock the Spark session
        spark = SparkSession.builder.appName("UrbanizationDataPipelineTest").getOrCreate()

        # Mock the extract function
        mock_extract.return_value = "/dbfs/tmp/urbanization_census_tract.csv"

        # Mock the transform function to return a mock DataFrame
        mock_transformed_df = MagicMock()
        mock_transform.return_value = mock_transformed_df

        # Mock the load function
        mock_load.return_value = None

        # Mock the query_table function to return a mock DataFrame
        mock_queried_df = MagicMock()
        mock_query_table.return_value = mock_queried_df

        # Mock the visualization functions
        mock_visualize_population.return_value = None
        mock_visualize_distribution.return_value = None

        # Run the main function
        with patch("main.SparkSession.builder.getOrCreate", return_value=spark):
            main()

        # Assert the functions were called
        mock_extract.assert_called_once()
        mock_transform.assert_called_once_with("/dbfs/tmp/urbanization_census_tract.csv".replace("/dbfs", "dbfs:"), spark)
        mock_load.assert_called_once_with(mock_transformed_df, output_path="output/urbanization_census_tract.parquet", file_format="parquet")
        mock_query_table.assert_called_once_with(table_name="urbanization_data", database="urbanization_db", limit=1000)
        mock_visualize_population.assert_called_once_with(mock_queried_df)
        mock_visualize_distribution.assert_called_once_with(mock_queried_df)

        # Stop the Spark session
        spark.stop()

if __name__ == "__main__":
    unittest.main()
