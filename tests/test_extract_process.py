import unittest
from unittest.mock import patch, mock_open, MagicMock
import os
import json
import pandas as pd
from src.extract_process import parse_timestamp, extract_and_process_data

class TestExtractAndProcessData(unittest.TestCase):

    @patch("extract_and_process.parser.isoparse")
    def test_parse_timestamp(self, mock_isoparse):
        mock_isoparse.return_value = "2021-09-01T00:00:00Z"
        timestamp = "2021-09-1T0:00:0Z"
        expected = "2021-09-01T00:00:00Z"
        result = parse_timestamp(timestamp)
        self.assertEqual(result, expected)

    @patch("builtins.open", new_callable=mock_open)
    @patch("json.load")
    @patch("os.makedirs")
    @patch("pandas.DataFrame.to_csv")
    @patch("pandas.DataFrame.to_sql")
    @patch("pandas.DataFrame")
    @patch("sqlalchemy.create_engine")
    def test_extract_and_process_data(self, mock_create_engine, mock_dataframe, mock_to_sql, mock_to_csv, mock_makedirs, mock_json_load, mock_open):
        mock_json_load.side_effect = [{"key": "value"}, {"key2": "value2"}]
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_df_instance = mock_dataframe.return_value

        extract_and_process_data()

        self.assertTrue(mock_open.called)
        self.assertTrue(mock_json_load.called)
        self.assertTrue(mock_makedirs.called)
        self.assertTrue(mock_to_sql.called)
        self.assertTrue(mock_to_csv.called)

if __name__ == "__main__":
    unittest.main()
