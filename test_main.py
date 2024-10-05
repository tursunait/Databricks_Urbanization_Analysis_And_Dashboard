"""
Test goes here

"""

import unittest
from unittest.mock import patch
from main import main


class TestMainScript(unittest.TestCase):

    @patch("mylib.extract.extract")
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
        main()
        mock_extract.assert_called_once_with("http://example.com", "data/example.csv")

    @patch("mylib.transform_load.load")
    @patch(
        "sys.argv", ["main.py", "transform_load", "--dataset", "data/urbanization.csv"]
    )
    def test_transform_load(self, mock_load):
        """Test the transform and load functionality."""
        main()
        mock_load.assert_called_once_with("data/urbanization.csv")

    @patch("mylib.query.create_rec")
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
        main()
        mock_create_rec.assert_called_once_with(
            1, "Alabama", "G0100010", 32.3182, -86.9023, 50000, 100.5, 1.0
        )

    @patch("mylib.query.update_rec")
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
        main()
        mock_update_rec.assert_called_once_with(
            1, "Alabama", "G0100010", 32.3182, -86.9023, 60000, 120.5, 2.0
        )

    @patch("mylib.query.delete_rec")
    @patch("sys.argv", ["main.py", "delete_rec", "G0100010"])
    def test_delete_rec(self, mock_delete_rec):
        """Test the delete record functionality."""
        main()
        mock_delete_rec.assert_called_once_with("G0100010")

    @patch("mylib.query.general_query")
    @patch("sys.argv", ["main.py", "general_query", "SELECT * FROM urbanizationDB;"])
    def test_general_query(self, mock_general_query):
        """Test the general query functionality."""
        main()
        mock_general_query.assert_called_once_with("SELECT * FROM urbanizationDB;")

    @patch("mylib.query.read_data")
    @patch("sys.argv", ["main.py", "read_data"])
    def test_read_data(self, mock_read_data):
        """Test the read data functionality."""
        mock_read_data.return_value = [
            ("data", "value1"),
            ("data", "value2"),
        ]  # Mocking returned data
        main()
        mock_read_data.assert_called_once()
        # Here you can check if the data is printed out correctly.
        # This part is optional and requires capturing stdout.


if __name__ == "__main__":
    unittest.main()
