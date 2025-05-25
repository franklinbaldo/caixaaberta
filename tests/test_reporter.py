import unittest
import pandas as pd
import os
import tempfile
import shutil
import io
import contextlib
from reporter import generate_report, format_currency # Assuming reporter.py is in the parent directory or PYTHONPATH is set

class TestReporter(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.mock_imoveis_br_path = os.path.join(self.test_dir, "imoveis_BR.csv")

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def _create_csv(self, data, columns, filepath=None):
        if filepath is None:
            filepath = self.mock_imoveis_br_path
        df = pd.DataFrame(data, columns=columns)
        df.to_csv(filepath, index=False)
        return filepath

    def test_valid_data_report(self):
        data = [
            ("SP", 100000.0, "Apartamento A"),
            ("SP", 150000.0, "Apartamento B"),
            ("RJ", 200000.0, "Casa C"),
            ("MG", 120000.0, "Apartamento D"),
            ("SP", None, "Terreno E"), # Test NaN price
        ]
        columns = ["estado", "preco", "descricao"]
        self._create_csv(data, columns)

        captured_output = io.StringIO()
        with contextlib.redirect_stdout(captured_output):
            generate_report(self.mock_imoveis_br_path)
        
        output = captured_output.getvalue()
        
        self.assertIn(f"Real Estate Data Report for {self.mock_imoveis_br_path}", output)
        self.assertIn("Total properties listed: 5", output)
        self.assertIn("Properties per state:", output)
        self.assertIn("SP: 3 properties", output)
        self.assertIn("RJ: 1 properties", output) # Note: generate_report produces "1 properties" not "1 property"
        self.assertIn("MG: 1 properties", output)
        self.assertIn("Average price per state:", output)
        # SP: (100000 + 150000) / 2 = 125000.0
        # RJ: 200000.0
        # MG: 120000.0
        self.assertIn(f"SP: {format_currency(125000.0)}", output)
        self.assertIn(f"RJ: {format_currency(200000.0)}", output)
        self.assertIn(f"MG: {format_currency(120000.0)}", output)

    def test_empty_csv_report(self):
        self._create_csv([], ["estado", "preco", "descricao"]) # Create an empty CSV with headers
        
        captured_output = io.StringIO()
        with contextlib.redirect_stdout(captured_output):
            generate_report(self.mock_imoveis_br_path)
        
        output = captured_output.getvalue()
        self.assertIn(f"{self.mock_imoveis_br_path} is empty. No statistics to generate.", output)

    def test_truly_empty_csv_report(self):
        # Create a completely empty file (0 bytes)
        with open(self.mock_imoveis_br_path, 'w') as f:
            pass # Creates an empty file
            
        captured_output = io.StringIO()
        with contextlib.redirect_stdout(captured_output):
            generate_report(self.mock_imoveis_br_path)
        
        output = captured_output.getvalue()
        # This will be caught by pd.errors.EmptyDataError
        self.assertIn(f"Error: {self.mock_imoveis_br_path} is empty or malformed.", output)


    def test_missing_csv_report(self):
        non_existent_file = os.path.join(self.test_dir, "non_existent.csv")
        captured_output = io.StringIO()
        with contextlib.redirect_stdout(captured_output):
            generate_report(non_existent_file)
        
        output = captured_output.getvalue()
        self.assertIn(f"Error: {non_existent_file} not found.", output)

    def test_missing_preco_column_report(self):
        data = [("SP", "Apartamento A"), ("RJ", "Casa C")]
        columns = ["estado", "descricao"] # Missing 'preco'
        self._create_csv(data, columns)
        
        captured_output = io.StringIO()
        with contextlib.redirect_stdout(captured_output):
            generate_report(self.mock_imoveis_br_path)
            
        output = captured_output.getvalue()
        self.assertIn(f"Error: 'preco' column is missing from {self.mock_imoveis_br_path}", output)

    def test_missing_estado_column_report(self):
        data = [(100000.0, "Apartamento A"), (200000.0, "Casa C")]
        columns = ["preco", "descricao"] # Missing 'estado'
        self._create_csv(data, columns)
        
        captured_output = io.StringIO()
        with contextlib.redirect_stdout(captured_output):
            generate_report(self.mock_imoveis_br_path)
            
        output = captured_output.getvalue()
        self.assertIn(f"Error: 'estado' column is missing from {self.mock_imoveis_br_path}", output)

    def test_format_currency_helper(self):
        self.assertEqual(format_currency(12345.67), "R$ 12,345.67") # Assuming default/fallback locale uses this
        self.assertEqual(format_currency(12345.6), "R$ 12,345.60")
        self.assertEqual(format_currency(12345), "R$ 12,345.00")
        self.assertEqual(format_currency(0.5), "R$ 0.50")
        self.assertEqual(format_currency(pd.NA), "N/A")
        self.assertEqual(format_currency(None), "N/A")
        # Test with a value that might cause issues if locale isn't set properly
        # but our function has fallbacks.
        self.assertTrue("R$" in format_currency(1000000.00))

    def test_geocoding_stats_mixed_success(self):
        data = [
            ("SP", 100000.0, "Desc A", 10.0, 20.0), # Geocoded
            ("SP", 150000.0, "Desc B", None, None), # Not geocoded
            ("RJ", 200000.0, "Desc C", 30.0, 40.0), # Geocoded
            ("MG", 120000.0, "Desc D", None, None), # Not geocoded
            ("MG", 130000.0, "Desc E", 50.0, 60.0), # Geocoded
            ("SP", 130000.0, "Desc F", 70.0, 80.0), # Geocoded
        ]
        columns = ["estado", "preco", "descricao", "latitude", "longitude"]
        self._create_csv(data, columns)

        captured_output = io.StringIO()
        with contextlib.redirect_stdout(captured_output):
            generate_report(self.mock_imoveis_br_path)
        output = captured_output.getvalue()

        self.assertIn("Geocoding Statistics:", output)
        # Overall: 4 geocoded out of 6 total = 66.7%
        self.assertIn("Overall geocoding success rate: 66.7% (4 out of 6 properties)", output)
        # SP: 2 geocoded out of 3 total = 66.7%
        self.assertIn("SP: 66.7% (2 out of 3 properties)", output)
        # RJ: 1 geocoded out of 1 total = 100.0%
        self.assertIn("RJ: 100.0% (1 out of 1 properties)", output)
        # MG: 1 geocoded out of 2 total = 50.0%
        self.assertIn("MG: 50.0% (1 out of 2 properties)", output)

    def test_geocoding_stats_no_success(self):
        data = [
            ("SP", 100000.0, "Desc A", None, None),
            ("RJ", 200000.0, "Desc C", None, None),
        ]
        columns = ["estado", "preco", "descricao", "latitude", "longitude"]
        self._create_csv(data, columns)

        captured_output = io.StringIO()
        with contextlib.redirect_stdout(captured_output):
            generate_report(self.mock_imoveis_br_path)
        output = captured_output.getvalue()
        
        self.assertIn("Overall geocoding success rate: 0.0% (0 out of 2 properties)", output)
        self.assertIn("SP: 0.0% (0 out of 1 properties)", output)
        self.assertIn("RJ: 0.0% (0 out of 1 properties)", output)

    def test_geocoding_stats_full_success(self):
        data = [
            ("SP", 100000.0, "Desc A", 10.0, 20.0),
            ("RJ", 200000.0, "Desc C", 30.0, 40.0),
        ]
        columns = ["estado", "preco", "descricao", "latitude", "longitude"]
        self._create_csv(data, columns)

        captured_output = io.StringIO()
        with contextlib.redirect_stdout(captured_output):
            generate_report(self.mock_imoveis_br_path)
        output = captured_output.getvalue()
        
        self.assertIn("Overall geocoding success rate: 100.0% (2 out of 2 properties)", output)
        self.assertIn("SP: 100.0% (1 out of 1 properties)", output)
        self.assertIn("RJ: 100.0% (1 out of 1 properties)", output)

    def test_geocoding_stats_missing_lat_lon_columns(self):
        data = [("SP", 100000.0, "Desc A"), ("RJ", 200000.0, "Desc C")]
        columns = ["estado", "preco", "descricao"] # No latitude/longitude columns
        self._create_csv(data, columns)

        captured_output = io.StringIO()
        with contextlib.redirect_stdout(captured_output):
            generate_report(self.mock_imoveis_br_path)
        output = captured_output.getvalue()
        
        self.assertIn("Latitude/Longitude columns not found. Geocoding statistics cannot be generated.", output)
        # Ensure the rest of the report is still there
        self.assertIn("Total properties listed: 2", output) 

    def test_geocoding_stats_with_empty_dataframe_after_loading(self):
        # This test is similar to test_empty_csv_report, but specifically checks geocoding output
        # Create a CSV with headers but no data rows
        self._create_csv([], ["estado", "preco", "descricao", "latitude", "longitude"])
        
        captured_output = io.StringIO()
        with contextlib.redirect_stdout(captured_output):
            generate_report(self.mock_imoveis_br_path) # Corrected variable name
        
        output = captured_output.getvalue()
        self.assertIn(f"{self.mock_imoveis_br_path} is empty. No statistics to generate.", output)
        # Ensure Geocoding Statistics section is not printed or indicates no data
        self.assertNotIn("Overall geocoding success rate:", output) # Should not attempt if df is empty


if __name__ == '__main__':
    # This allows running the tests directly from this file
    # However, it's better to run using 'python -m unittest discover tests' from root
    unittest.main()
