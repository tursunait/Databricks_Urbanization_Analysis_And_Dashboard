"""
Test goes here

"""

import unittest
from unittest.mock import patch
import main


class TestMainScript(unittest.TestCase):

    @patch("main.extract")  # Mock where extract is used, i.e., in `main.py`
    @patch(
        "sys.argv",
        [
            "main.py",
            "extract",
            "--url",
            "http://example.com",
            "--file_path",
            "data/example.csv",
        ],
    )
    def test_extract(self, mock_extract):
        """Test the extract functionality."""
        main.main()  # Call the main function to trigger the logic
        mock_extract.assert_called_once_with("http://example.com", "data/example.csv")

    @patch("main.load")  # Mock where load is used, i.e., in `main.py`
    @patch(
        "sys.argv", ["main.py", "transform_load", "--dataset", "data/urbanization.csv"]
    )
    def test_transform_load(self, mock_load):
        """Test the transform and load functionality."""
        main.main()  # Call the main function to trigger the logic
        mock_load.assert_called_once_with("data/urbanization.csv")

    @patch("main.create_rec")  # Mock where create_rec is used, i.e., in `main.py`
    @patch(
        "sys.argv",
        [
            "main.py",
            "create_rec",
            "1",
            "Alabama",
            "G0100010",
            "32.3182",
            "-86.9023",
            "50000",
            "100.5",
            "1.0",
        ],
    )
    def test_create_rec(self, mock_create_rec):
        """Test the create record functionality."""
        main.main()  # Call the main function to trigger the logic
        mock_create_rec.assert_called_once_with(
            1, "Alabama", "G0100010", 32.3182, -86.9023, 50000, 100.5, 1.0
        )

    @patch("main.update_rec")  # Mock where update_rec is used, i.e., in `main.py`
    @patch(
        "sys.argv",
        [
            "main.py",
            "update_rec",
            "1",
            "Alabama",
            "G0100010",
            "32.3182",
            "-86.9023",
            "60000",
            "120.5",
            "2.0",
        ],
    )
    def test_update_rec(self, mock_update_rec):
        """Test the update record functionality."""
        main.main()  # Call the main function to trigger the logic
        mock_update_rec.assert_called_once_with(
            1, "Alabama", "G0100010", 32.3182, -86.9023, 60000, 120.5, 2.0
        )

    @patch("main.delete_rec")  # Mock where delete_rec is used, i.e., in `main.py`
    @patch("sys.argv", ["main.py", "delete_rec", "G0100010"])
    def test_delete_rec(self, mock_delete_rec):
        """Test the delete record functionality."""
        main.main()  # Call the main function to trigger the logic
        mock_delete_rec.assert_called_once_with("G0100010")

    @patch("main.general_query")  # Mock where general_query is used, i.e., in `main.py`
    @patch("sys.argv", ["main.py", "general_query", "SELECT * FROM urbanizationDB;"])
    def test_general_query(self, mock_general_query):
        """Test the general query functionality."""
        main.main()  # Call the main function to trigger the logic
        mock_general_query.assert_called_once_with("SELECT * FROM urbanizationDB;")

    @patch("main.read_data")  # Mock where read_data is used, i.e., in `main.py`
    @patch("sys.argv", ["main.py", "read_data"])
    def test_read_data(self, mock_read_data):
        """Test the read data functionality."""
        mock_read_data.return_value = [
            ("data", "value1"),
            ("data", "value2"),
        ]  # Mock returned data
        main.main()  # Call the main function to trigger the logic
        mock_read_data.assert_called_once()
        # Optionally check if the correct data was printed
        self.assertEqual(
            mock_read_data.return_value, [("data", "value1"), ("data", "value2")]
        )


if __name__ == "__main__":
    unittest.main()
