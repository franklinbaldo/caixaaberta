import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pathlib import Path
import os
import sys

# Add src to sys.path since the original tests relied on it
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from fetch_data import process_local_data

@patch("fetch_data.ibis")
@patch("fetch_data.pd.read_csv")
@patch("fetch_data.Path.glob")
@patch("fetch_data.get_coordinates_for_address")
def test_process_local_data(mock_get_coords, mock_glob, mock_read_csv, mock_ibis, tmp_path):
    # Setup mock data for CSVs
    mock_glob.return_value = [Path("data/imoveis_SP.csv")]

    mock_df = pd.DataFrame({
        "endereco": ["Rua A", "Rua B"],
        "bairro": ["Bairro X", None],
        "cidade": ["São Paulo", "São Paulo"],
        "estado": ["SP", "SP"],
        "link": ["link1", "link2"],
        "latitude": [None, pd.NA],
        "longitude": [pd.NA, None],
        "foto": ["foto1.jpg", "foto2.jpg"]
    })
    mock_read_csv.return_value = mock_df

    # Setup ibis mock
    mock_conn = MagicMock()
    mock_ibis.duckdb.connect.return_value = mock_conn
    mock_table = MagicMock()
    mock_conn.table.return_value = mock_table
    mock_ibis.union.return_value = mock_table

    # Chain mutate, drop_null, distinct to return mock_table
    mock_table.mutate.return_value = mock_table
    mock_table.drop_null.return_value = mock_table
    mock_table.distinct.return_value = mock_table

    # to_pandas returns a fresh copy of mock_df
    mock_table.to_pandas.return_value = mock_df.copy()

    # Setup mock geocoding (should process 2 rows)
    mock_get_coords.side_effect = [(-23.5, -46.6), (-23.6, -46.7)]

    with patch("fetch_data.OUTPUT_DIR", str(tmp_path)):
        with patch.dict(os.environ, {"GEOCODER_KEY": "test_key"}):
            process_local_data()

    # Verify that parquet file was created
    output_file = tmp_path / "imoveis_geocoded.parquet"
    assert output_file.exists()

    # Read output and verify geocoding was applied
    result_df = pd.read_parquet(output_file)
    assert len(result_df) == 2
    assert result_df.iloc[0]["latitude"] == -23.5
    assert result_df.iloc[0]["longitude"] == -46.6
    assert result_df.iloc[1]["latitude"] == -23.6
    assert result_df.iloc[1]["longitude"] == -46.7

@patch("fetch_data.Path.glob")
def test_process_local_data_no_csvs(mock_glob, capsys):
    mock_glob.return_value = []

    process_local_data()

    captured = capsys.readouterr()
    assert "Nenhum arquivo CSV encontrado" in captured.out
