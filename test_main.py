import unittest
from unittest.mock import patch, MagicMock
import sys
import main  # Import the main script

# Assuming mylib.extract, mylib.transform_load, and mylib.query.general_query are available in the path
from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import general_query


class TestMain(unittest.TestCase):

    @patch("main.extract")
    def test_extract_action(self, mock_extract):
        """Test extract action"""
        test_args = ["main.py", "extract"]

        with patch.object(sys, "argv", test_args):
            main.main()
            mock_extract.assert_called_once()  # Check if extract is called

    @patch("main.load")
    def test_transform_load_action(self, mock_load):
        """Test transform_load action"""
        test_args = ["main.py", "transform_load"]

        with patch.object(sys, "argv", test_args):
            main.main()
            mock_load.assert_called_once()  # Check if load is called

    @patch("main.general_query")
    def test_general_query_action(self, mock_general_query):
        """Test general_query action with a query argument"""
        test_query = "SELECT * FROM table"
        test_args = ["main.py", "general_query", test_query]

        with patch.object(sys, "argv", test_args):
            main.main()
            mock_general_query.assert_called_once_with(
                test_query
            )  # Check if general_query is called with query

    @patch("builtins.print")
    def test_invalid_action(self, mock_print):
        """Test handling of invalid action"""
        test_args = ["main.py", "invalid_action"]

        with patch.object(sys, "argv", test_args):
            main.main()
            mock_print.assert_called_with(
                "Unknown action: invalid_action"
            )  # Check if appropriate error message is printed


if __name__ == "__main__":
    unittest.main()
