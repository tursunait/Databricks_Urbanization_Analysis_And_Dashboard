import unittest
from unittest.mock import patch
import main


class TestMain(unittest.TestCase):

    @patch("main.extract")
    @patch("sys.argv", ["main.py", "extract"])
    def test_extract(self, mock_extract):
        """Test the extract functionality."""
        main.main()
        mock_extract.assert_called_once()

    @patch("main.load")
    @patch("sys.argv", ["main.py", "transform_load"])
    def test_transform_load(self, mock_load):
        """Test the transform and load functionality."""
        main.main()
        mock_load.assert_called_once()

    @patch("main.general_query")
    @patch(
        "sys.argv",
        ["main.py", "general_query", "SELECT * FROM default.urbanizationdb LIMIT 10"],
    )
    def test_general_query(self, mock_general_query):
        """Test the general query functionality."""
        main.main()
        mock_general_query.assert_called_once_with(
            "SELECT * FROM default.urbanizationdb LIMIT 10"
        )


if __name__ == "__main__":
    unittest.main()
