import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import os
import tempfile
import shutil
from datetime import date, datetime, timedelta
import numpy as np # For pd.NA comparison if needed
from pathlib import Path # Added for use in setUp's side_effect_path

# Assuming psa.py, etl.py, processador_caixa.py are structured in a way that they can be imported.
# If psa.py is in the root, and tests is a subdir, add root to path for imports or use relative.
# For simplicity, assume they are discoverable.
from psa import update_records, cols as psa_cols_definition, etl_cols as psa_etl_cols_definition, sorting_cols as psa_sorting_cols_definition
# processador_caixa is used by psa, so its cleaning is tested via psa.

class TestPSA(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.history_file_path = os.path.join(self.test_dir, "imoveis_BR_test.csv")
        self.mock_data_root_dir = os.path.join(self.test_dir, "app_root") # Simulate project root
        self.mock_psa_data_dir = os.path.join(self.mock_data_root_dir, "data") # This is what Path("data/") in psa.py will point to
        os.makedirs(self.mock_psa_data_dir)

        # Patching psa.file_path which is used internally by update_records to save the CSV
        self.file_path_patcher = patch('psa.file_path', self.history_file_path)
        self.mock_file_path = self.file_path_patcher.start()

        # Patching Path("data/") specifically its glob method
        # When psa.py calls Path("data/").glob("imoveis_*.csv"), we want it to look in self.mock_psa_data_dir
        # We need to patch 'psa.Path'
        self.path_patcher = patch('psa.Path')
        self.mock_path_constructor = self.path_patcher.start()

        def side_effect_path(path_arg):
            if str(path_arg) == "data/": # The specific path used in psa.py for globbing
                # Return a MagicMock that has a glob method configured for our test data dir
                mock_data_path_instance = MagicMock()
                # Simulate glob by listing files in our mock_psa_data_dir
                glob_results = [os.path.join(self.mock_psa_data_dir, f) for f in os.listdir(self.mock_psa_data_dir) if f.startswith("imoveis_") and f.endswith(".csv")]
                # The glob method should return an iteratable of Path objects (or strings that pd.read_csv can handle)
                mock_data_path_instance.glob.return_value = glob_results
                return mock_data_path_instance
            elif str(path_arg) == self.history_file_path: # For reading history and creating output file
                # Return a real Path object or a mock that behaves like one for file operations
                # For output, psa.py uses Path(output) then .to_csv, which should be fine with a string.
                # For input (history reading), Path(file) is used.
                # Let's return a mock that can handle .exists() and .stat().st_size for history loading
                mock_history_path_instance = MagicMock()
                real_path = Path(path_arg) # Intentionally using original Path for actual file ops
                mock_history_path_instance.exists.return_value = real_path.exists()
                mock_history_path_instance.stat.return_value.st_size = real_path.stat().st_size if real_path.exists() else 0
                # Make it usable as a string for pd.read_csv and to_csv
                mock_history_path_instance.__str__.return_value = str(path_arg)
                mock_history_path_instance.parent.mkdir = MagicMock() # Mock parent.mkdir call
                return mock_history_path_instance
            # Fallback to actual Path for other uses if any (though not expected for psa.py's current structure)
            return Path(path_arg) # Intentionally using original Path for other cases

        self.mock_path_constructor.side_effect = side_effect_path
        
        # Ensure 'cols', 'etl_cols', 'sorting_cols' used by the test match those in psa.py
        self.etl_cols = psa_etl_cols_definition
        self.cols = psa_cols_definition # Includes etl_cols + timestamps
        self.sorting_cols = psa_sorting_cols_definition


    def tearDown(self):
        shutil.rmtree(self.test_dir)
        self.file_path_patcher.stop()
        self.path_patcher.stop()

    def _create_state_csv(self, filename_suffix, data, columns):
        filepath = os.path.join(self.mock_psa_data_dir, f"imoveis_{filename_suffix}.csv")
        df = pd.DataFrame(data, columns=columns)
        df.to_csv(filepath, index=False)
        return filepath

    def assertDataframeHasFloatColumns(self, df, cols_to_check):
        for col_name in cols_to_check:
            self.assertTrue(pd.api.types.is_float_dtype(df[col_name]), f"Column '{col_name}' is not float, it is {df[col_name].dtype}")

    def test_financial_data_cleaning(self):
        data = [("SP", "Some desc", "R$ 1.234,56", "R$ 2.000,00", "50,00%")]
        # Ensure column names match etl_cols for 'preco', 'avaliacao', 'desconto'
        state_cols = ['estado', 'descricao', 'preco', 'avaliacao', 'desconto'] 
        # Add other etl_cols with dummy values
        for col in self.etl_cols:
            if col not in state_cols:
                data[0] = data[0] + (f"dummy_{col}",)
                state_cols.append(col)
        
        self._create_state_csv("SP_finance", [data[0]], state_cols)
        
        update_records(output=self.history_file_path) # psa.file_path is patched
        
        result_df = pd.read_csv(self.history_file_path)
        self.assertFalse(result_df.empty)
        self.assertDataframeHasFloatColumns(result_df, ['preco', 'avaliacao', 'desconto'])
        self.assertAlmostEqual(result_df['preco'].iloc[0], 1234.56)
        self.assertAlmostEqual(result_df['avaliacao'].iloc[0], 2000.00)
        self.assertAlmostEqual(result_df['desconto'].iloc[0], 0.50)

    def test_first_time_seen_new_records(self):
        # Ensure history file does not exist initially for a clean test
        if os.path.exists(self.history_file_path):
            os.remove(self.history_file_path)

        data = [("SP", "Desc A", 100.0)]
        state_cols = ['estado', 'descricao', 'preco']
        for col in self.etl_cols:
            if col not in state_cols:
                data[0] = data[0] + (f"dummy_{col}",)
                state_cols.append(col)
        self._create_state_csv("SP_new", [data[0]], state_cols)

        update_records(output=self.history_file_path)
        result_df = pd.read_csv(self.history_file_path)
        
        self.assertEqual(len(result_df), 1)
        self.assertIn('first_time_seen', result_df.columns)
        self.assertIn('not_seen_since', result_df.columns)
        self.assertEqual(result_df['first_time_seen'].iloc[0], date.today().isoformat())
        self.assertTrue(pd.isna(result_df['not_seen_since'].iloc[0]))

    def test_existing_records_no_change(self):
        # Run 1: Create initial record
        data1 = [("RJ", "Desc B", 200.0)]
        state_cols = ['estado', 'descricao', 'preco']
        for col in self.etl_cols:
            if col not in state_cols:
                data1[0] = data1[0] + (f"dummy_{col}",)
                state_cols.append(col)
        self._create_state_csv("RJ_nochange", [data1[0]], state_cols)
        
        update_records(output=self.history_file_path)
        result_df1 = pd.read_csv(self.history_file_path)
        first_seen_run1 = result_df1['first_time_seen'].iloc[0]

        # Run 2: Same data
        # Simulate time passing by a bit for the 'now' timestamp in psa.py if needed, though not strictly for this test
        # For this test, ensure the data files are "re-globbed" by clearing and re-creating
        os.remove(os.path.join(self.mock_psa_data_dir, "imoveis_RJ_nochange.csv"))
        self._create_state_csv("RJ_nochange_run2", [data1[0]], state_cols) # Re-create with different name to ensure glob re-reads

        update_records(output=self.history_file_path)
        result_df2 = pd.read_csv(self.history_file_path)

        self.assertEqual(len(result_df2), 1)
        self.assertEqual(result_df2['first_time_seen'].iloc[0], first_seen_run1)
        self.assertTrue(pd.isna(result_df2['not_seen_since'].iloc[0]))

    def test_records_disappear(self):
        # Run 1: Records A and B
        record_A_full = tuple(["SP", "Desc A", 100.0] + [f"dummy_A_{col}" for col in self.etl_cols if col not in ['estado', 'descricao', 'preco']])
        record_B_full = tuple(["RJ", "Desc B", 200.0] + [f"dummy_B_{col}" for col in self.etl_cols if col not in ['estado', 'descricao', 'preco']])
        state_cols_full = self.etl_cols # Assume order matches

        self._create_state_csv("Multi_disappear1", [record_A_full, record_B_full], state_cols_full)
        update_records(output=self.history_file_path)
        
        # Run 2: Only Record A
        # Clear old state files, create new one
        for f in os.listdir(self.mock_psa_data_dir): os.remove(os.path.join(self.mock_psa_data_dir, f))
        self._create_state_csv("Multi_disappear2", [record_A_full], state_cols_full)
        update_records(output=self.history_file_path)
        result_df = pd.read_csv(self.history_file_path)

        self.assertEqual(len(result_df), 2)
        record_A_output = result_df[result_df['descricao'] == "Desc A"]
        record_B_output = result_df[result_df['descricao'] == "Desc B"]
        
        self.assertTrue(pd.isna(record_A_output['not_seen_since'].iloc[0]))
        self.assertEqual(record_B_output['not_seen_since'].iloc[0], date.today().isoformat())

    def test_records_reappear(self):
        # Run 1: Record A and B
        record_A_full = tuple(["SP", "Desc A Reappear", 100.0] + [f"dummy_A_{col}" for col in self.etl_cols if col not in ['estado', 'descricao', 'preco']])
        record_B_full = tuple(["RJ", "Desc B Reappear", 200.0] + [f"dummy_B_{col}" for col in self.etl_cols if col not in ['estado', 'descricao', 'preco']])
        state_cols_full = self.etl_cols

        self._create_state_csv("Multi_reappear1", [record_A_full, record_B_full], state_cols_full)
        update_records(output=self.history_file_path)
        df_run1 = pd.read_csv(self.history_file_path)
        first_seen_B_run1 = df_run1[df_run1['descricao'] == "Desc B Reappear"]['first_time_seen'].iloc[0]

        # Run 2: Only Record A (B disappears)
        for f in os.listdir(self.mock_psa_data_dir): os.remove(os.path.join(self.mock_psa_data_dir, f))
        self._create_state_csv("Multi_reappear2", [record_A_full], state_cols_full)
        update_records(output=self.history_file_path)
        # df_run2 = pd.read_csv(self.history_file_path) # We need to check B's not_seen_since is set

        # Run 3: Record A and B reappear
        for f in os.listdir(self.mock_psa_data_dir): os.remove(os.path.join(self.mock_psa_data_dir, f))
        self._create_state_csv("Multi_reappear3", [record_A_full, record_B_full], state_cols_full)
        update_records(output=self.history_file_path)
        result_df = pd.read_csv(self.history_file_path)
        
        record_B_output = result_df[result_df['descricao'] == "Desc B Reappear"]
        self.assertTrue(pd.isna(record_B_output['not_seen_since'].iloc[0]))
        self.assertEqual(record_B_output['first_time_seen'].iloc[0], first_seen_B_run1)

    def test_empty_state_file_handling(self):
        # One valid, one empty
        valid_data = [("SP", "Valid Data", 100.0)]
        state_cols = ['estado', 'descricao', 'preco']
        for col in self.etl_cols:
            if col not in state_cols:
                valid_data[0] = valid_data[0] + (f"dummy_{col}",)
                state_cols.append(col)

        self._create_state_csv("SP_valid", [valid_data[0]], state_cols)
        # Create an empty CSV (only headers)
        empty_state_file_path = os.path.join(self.mock_psa_data_dir, "imoveis_EMPTY.csv")
        pd.DataFrame(columns=state_cols).to_csv(empty_state_file_path, index=False)

        update_records(output=self.history_file_path)
        result_df = pd.read_csv(self.history_file_path)
        self.assertEqual(len(result_df), 1)
        self.assertEqual(result_df['descricao'].iloc[0], "Valid Data")

    def test_state_file_missing_financial_columns(self):
        # 'preco' is missing, 'avaliacao' and 'desconto' will be missing too
        data = [("SP", "No Price Data")] 
        state_cols = ['estado', 'descricao'] 
        # Add other etl_cols (non-financial)
        for col in self.etl_cols:
            if col not in state_cols and col not in ['preco', 'avaliacao', 'desconto']:
                data[0] = data[0] + (f"dummy_{col}",)
                state_cols.append(col)
        self._create_state_csv("SP_missing_price", [data[0]], state_cols)

        update_records(output=self.history_file_path)
        result_df = pd.read_csv(self.history_file_path)
        self.assertEqual(len(result_df), 1)
        self.assertTrue(pd.isna(result_df['preco'].iloc[0]))
        self.assertTrue(pd.isna(result_df['avaliacao'].iloc[0]))
        self.assertTrue(pd.isna(result_df['desconto'].iloc[0]))

    def test_empty_initial_history_file(self):
        # Ensure history file does not exist
        if os.path.exists(self.history_file_path):
            os.remove(self.history_file_path)

        data = [("NY", "New History", 500.0)]
        state_cols = ['estado', 'descricao', 'preco']
        for col in self.etl_cols:
            if col not in state_cols:
                data[0] = data[0] + (f"dummy_{col}",)
                state_cols.append(col)
        self._create_state_csv("NY_newhist", [data[0]], state_cols)
        
        update_records(output=self.history_file_path)
        self.assertTrue(os.path.exists(self.history_file_path))
        result_df = pd.read_csv(self.history_file_path)
        self.assertEqual(len(result_df), 1)
        self.assertEqual(result_df['descricao'].iloc[0], "New History")

    def test_all_state_files_empty(self):
        # Create a couple of empty state files
        state_cols = self.etl_cols 
        empty_state_file_path1 = os.path.join(self.mock_psa_data_dir, "imoveis_EMPTY1.csv")
        pd.DataFrame(columns=state_cols).to_csv(empty_state_file_path1, index=False)
        empty_state_file_path2 = os.path.join(self.mock_psa_data_dir, "imoveis_EMPTY2.csv")
        pd.DataFrame(columns=state_cols).to_csv(empty_state_file_path2, index=False)

        update_records(output=self.history_file_path)
        result_df = pd.read_csv(self.history_file_path)
        # Expecting an empty DataFrame (or one with only headers if history was also empty)
        # The psa.py script initializes history with columns if the file is empty.
        # And current_data will be an empty DF if no valid state data.
        # The concat of two empty DFs (or one empty, one with only headers from an empty history)
        # should result in a DF that is effectively empty of data rows.
        # The current psa.py ensures 'first_time_seen' and 'not_seen_since' are in history even if empty.
        # And final_df selects sorting_cols + these two.
        
        # Check if the dataframe is empty of actual data rows
        self.assertTrue(result_df.empty or len(result_df) == 0, "DataFrame should be empty of data rows.")
        # Check if expected columns are present (they should be, due to how empty history/current_data are handled)
        expected_final_cols = set(self.sorting_cols + ['first_time_seen', 'not_seen_since'])
        # Allow for result_df to be completely empty (no columns) if no state files and no history
        if not result_df.empty:
            self.assertTrue(expected_final_cols.issubset(set(result_df.columns)))
        else:
            # If it's empty, it means no data rows, which is fine.
            # The assertion on len(result_df) == 0 already covers this.
            pass


    @patch('psa.get_coordinates_for_address')
    def test_geocoding_new_records(self, mock_get_coords):
        mock_get_coords.return_value = (10.0, 20.0)
        
        # Define data for a state CSV. Ensure address components are clear.
        # These etl_cols are the 'etl_cols_original' in psa.py
        # 'latitude', 'longitude' will be added by psa.py with NA initially for these new records.
        data = [("SP", "Rua Teste, 123", "Bairro Teste", "Cidade Teste", "Desc A", 100.0)] 
        current_etl_cols = ['estado', 'endereco', 'bairro', 'cidade', 'descricao', 'preco']
        
        # Add other etl_cols_original with dummy values if they are not in current_etl_cols
        full_test_data_row = list(data[0])
        for col_name in self.etl_cols: # self.etl_cols is psa_etl_cols_definition (etl_cols_original from etl.py)
            if col_name not in current_etl_cols:
                full_test_data_row.append(f"dummy_{col_name}")
                current_etl_cols.append(col_name)
        
        self._create_state_csv("SP_geotest", [tuple(full_test_data_row)], current_etl_cols)

        update_records(output=self.history_file_path)
        result_df = pd.read_csv(self.history_file_path)

        self.assertEqual(len(result_df), 1)
        self.assertAlmostEqual(result_df['latitude'].iloc[0], 10.0)
        self.assertAlmostEqual(result_df['longitude'].iloc[0], 20.0)
        
        expected_address_str = "Rua Teste, 123, Bairro Teste, Cidade Teste, SP"
        mock_get_coords.assert_called_once_with(expected_address_str)

    @patch('psa.get_coordinates_for_address')
    def test_geocoding_preserves_existing_coords_if_no_update(self, mock_get_coords):
        # History: Record H1 with existing lat/lon
        history_data = [("RJ", "Av Principal, 456", "Centro", "Rio de Janeiro", "Desc H", 300.0, 1.23, 4.56, date(2023,1,1).isoformat(), None)]
        # Columns for history: etl_cols_original + lat, lon, first_seen, not_seen
        history_cols = self.etl_cols + ['latitude', 'longitude', 'first_time_seen', 'not_seen_since']
        
        # Create the history file directly for this test
        history_df = pd.DataFrame(history_data, columns=history_cols)
        history_df.to_csv(self.history_file_path, index=False)

        # State data: No new files, or an empty file (so H1 is purely from history)
        self._create_state_csv("EMPTY_geopreserve", [], self.etl_cols)

        update_records(output=self.history_file_path)
        result_df = pd.read_csv(self.history_file_path)
        
        self.assertEqual(len(result_df), 1)
        self.assertAlmostEqual(result_df['latitude'].iloc[0], 1.23)
        self.assertAlmostEqual(result_df['longitude'].iloc[0], 4.56)
        mock_get_coords.assert_not_called() # Should not be called as lat/lon were present

    @patch('psa.get_coordinates_for_address')
    def test_geocoding_called_for_record_updated_without_coords(self, mock_get_coords):
        mock_get_coords.return_value = (99.0, 88.0) # New coords if geocoding is triggered

        # History: Record H1 with existing lat/lon
        history_data = [("SP", "Rua Antiga, 1", "Velho", "Sao Paulo", "Desc Hist", 1.0, 11.1, 22.2, date(2023,1,1).isoformat(), None)]
        history_cols = self.etl_cols + ['latitude', 'longitude', 'first_time_seen', 'not_seen_since']
        history_df = pd.DataFrame(history_data, columns=history_cols)
        history_df.to_csv(self.history_file_path, index=False)

        # State data: Same record as H1 (based on etl_cols_original), but price changed.
        # psa.py initializes lat/lon for state data to NA.
        # When this "updates" H1, the NA lat/lon from current_data will overwrite H1's,
        # then it should be geocoded.
        state_data_row = ["SP", "Rua Antiga, 1", "Velho", "Sao Paulo", "Desc Hist", 2.0] # Price updated
        current_etl_cols = ['estado', 'endereco', 'bairro', 'cidade', 'descricao', 'preco']
        full_state_data_row = list(state_data_row)
        for col_name in self.etl_cols:
             if col_name not in current_etl_cols:
                full_state_data_row.append(f"dummy_{col_name}") # Use original dummy for other fields
                current_etl_cols.append(col_name)
        self._create_state_csv("SP_update_geotest", [tuple(full_state_data_row)], current_etl_cols)

        update_records(output=self.history_file_path)
        result_df = pd.read_csv(self.history_file_path)

        self.assertEqual(len(result_df), 1)
        self.assertAlmostEqual(result_df['latitude'].iloc[0], 99.0) # Should have new geocoded coords
        self.assertAlmostEqual(result_df['longitude'].iloc[0], 88.0)
        
        expected_address_str = "Rua Antiga, 1, Velho, Sao Paulo, SP"
        mock_get_coords.assert_called_once_with(expected_address_str)


    @patch('psa.get_coordinates_for_address')
    def test_geocoding_failure_for_record(self, mock_get_coords):
        mock_get_coords.return_value = (None, None)
        
        data = [("BR", "Rua Sem Coordenadas, 000", "Distante", "Lugar Nenhum", "Desc C", 250.0)]
        current_etl_cols = ['estado', 'endereco', 'bairro', 'cidade', 'descricao', 'preco']
        full_test_data_row = list(data[0])
        for col_name in self.etl_cols:
            if col_name not in current_etl_cols:
                full_test_data_row.append(f"dummy_{col_name}")
                current_etl_cols.append(col_name)
        self._create_state_csv("BR_nogeotest", [tuple(full_test_data_row)], current_etl_cols)

        update_records(output=self.history_file_path)
        result_df = pd.read_csv(self.history_file_path)

        self.assertEqual(len(result_df), 1)
        self.assertTrue(pd.isna(result_df['latitude'].iloc[0]))
        self.assertTrue(pd.isna(result_df['longitude'].iloc[0]))
        
        expected_address_str = "Rua Sem Coordenadas, 000, Distante, Lugar Nenhum, BR"
        mock_get_coords.assert_called_once_with(expected_address_str)


if __name__ == '__main__':
    # unittest.main() should be sufficient if Path is imported globally.
    unittest.main()
