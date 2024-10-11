import unittest
from unittest.mock import patch
import main  # Only keep the main import for calling main.main()


class TestMain(unittest.TestCase):

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

    @patch("main.general_query")  # Corrected the function name to general_query
    @patch(
        "sys.argv",
        [
            "main.py",
            "general_query",
            """SELECT *
            FROM default.urbanizationdb 
            LIMIT 10""",
        ],
    )
    def test_general_query(self, mock_general_query):
        """Test the general query functionality."""
        main.main()  # Call the main function to trigger the logic
        mock_general_query.assert_called_once_with(
            """SELECT * FROM default.urbanizationdb LIMIT 10"""
        )


if __name__ == "__main__":
    unittest.main()
