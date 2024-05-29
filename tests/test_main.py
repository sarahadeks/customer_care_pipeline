import unittest
from unittest.mock import patch, MagicMock, mock_open
from src.main import create_tables, load_raw_data, transform_data, load_transformed_data

class TestMainFunctions(unittest.TestCase):

    @patch("main.create_engine")
    def test_create_tables(self, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        create_tables()

        self.assertTrue(mock_create_engine.called)
        self.assertTrue(mock_engine.connect.called)
        self.assertTrue(mock_engine.connect().execute.called)

    @patch("builtins.open", new_callable=mock_open)
    @patch("json.load")
    @patch("pandas.DataFrame.to_sql")
    @patch("pandas.DataFrame")
    @patch("sqlalchemy.create_engine")
    def test_load_raw_data(self, mock_create_engine, mock_dataframe, mock_to_sql, mock_json_load, mock_open):
        mock_json_load.side_effect = [{"key": "value"}, {"key2": "value2"}]
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_df_instance = mock_dataframe.return_value

        load_raw_data()

        self.assertTrue(mock_open.called)
        self.assertTrue(mock_json_load.called)
        self.assertTrue(mock_to_sql.called)

    @patch("builtins.open", new_callable=mock_open, read_data="SELECT * FROM table;")
    @patch("sqlalchemy.create_engine")
    def test_transform_data(self, mock_create_engine, mock_open):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        transform_data()

        self.assertTrue(mock_open.called)
        self.assertTrue(mock_create_engine.called)
        self.assertTrue(mock_engine.connect.called)
        self.assertTrue(mock_engine.connect().execute.called)

    @patch("pandas.read_sql")
    @patch("pandas.DataFrame.to_csv")
    @patch("sqlalchemy.create_engine")
    def test_load_transformed_data(self, mock_create_engine, mock_to_csv, mock_read_sql):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_df_instance = MagicMock()
        mock_read_sql.return_value = mock_df_instance

        load_transformed_data()

        self.assertTrue(mock_create_engine.called)
        self.assertTrue(mock_read_sql.called)
        self.assertTrue(mock_to_csv.called)

if __name__ == "__main__":
    unittest.main()
