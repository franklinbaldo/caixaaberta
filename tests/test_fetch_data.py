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
def test_process_local_data_no_csvs(mock_glob):
    mock_glob.return_value = []

    with pytest.raises(FileNotFoundError, match="Nenhum arquivo CSV encontrado"):
        process_local_data()


CAIXA_CSV = (
    "\n Lista de Imóveis da Caixa;;Data de geração:;31/08/2026;;;;;;;\n"
    " N° do imóvel;UF;Cidade;Bairro;Endereço;Preço;Valor de avaliação;Desconto;"
    "Financiamento;Descrição;Modalidade de venda;Link de acesso\n"
    " 10218509 ;RO ;PORTO VELHO ;LIBERDADE ;AVENIDA PARÁ, N. 00 ;99.743,11;"
    "170.000,00;41.33;Não;Terreno.;Venda Direta Online;https://exemplo/1\n"
    " 10307865 ;RO ;CACOAL ;CENTRO ;RUA IJAD DID, N. SN ;1.160.000,00;"
    "1.160.000,00;0.00;Sim;Casa.;Leilão SFI;https://exemplo/2\n"
    ";;;;;;;;;;;\n"
)


def test_parse_caixa_csv_maps_columns_and_values():
    from fetch_data import CSV_COLUMNS, parse_caixa_csv

    df = parse_caixa_csv(CAIXA_CSV.encode("latin-1"))

    assert list(df.columns) == CSV_COLUMNS
    assert len(df) == 2
    assert df.iloc[0]["link"] == "10218509"
    assert df.iloc[0]["cidade"] == "PORTO VELHO"
    assert df.iloc[0]["estado"] == "RO"
    assert df.iloc[0]["preco"] == pytest.approx(99743.11)
    assert df.iloc[0]["avaliacao"] == pytest.approx(170000.0)
    assert df.iloc[0]["desconto"] == pytest.approx(41.33)
    assert df.iloc[1]["modalidade"] == "Leilão SFI"


def test_parse_caixa_csv_without_header_fails():
    from fetch_data import FetchError, parse_caixa_csv

    with pytest.raises(FetchError, match="Cabeçalho"):
        parse_caixa_csv("qualquer coisa;sem cabeçalho\n")


def test_fetch_all_states_writes_csvs(tmp_path, requests_mock):
    from fetch_data import fetch_all_states

    url_base = "https://exemplo.test/Lista_imoveis_{}.csv"
    for uf in ("RO", "SP"):
        requests_mock.get(
            url_base.format(uf), content=CAIXA_CSV.encode("latin-1")
        )

    frames = fetch_all_states(
        url_base=url_base, input_dir=str(tmp_path), ufs=["RO", "SP"], delay=0
    )

    assert set(frames) == {"RO", "SP"}
    written = pd.read_csv(tmp_path / "imoveis_RO.csv")
    assert len(written) == 2
    assert written.iloc[0]["link"] == 10218509


def test_fetch_all_states_fails_on_empty_state(tmp_path, requests_mock):
    from fetch_data import FetchError, fetch_all_states

    url_base = "https://exemplo.test/Lista_imoveis_{}.csv"
    requests_mock.get(
        url_base.format("RO"), content=CAIXA_CSV.encode("latin-1")
    )
    empty = CAIXA_CSV.split("\n")[:3]
    requests_mock.get(
        url_base.format("SP"), content="\n".join(empty).encode("latin-1")
    )

    with pytest.raises(FetchError, match="SP"):
        fetch_all_states(
            url_base=url_base, input_dir=str(tmp_path), ufs=["RO", "SP"], delay=0
        )

    assert not list(tmp_path.glob("*.csv"))


def test_fetch_all_states_fails_on_http_error(tmp_path, requests_mock):
    from fetch_data import FetchError, fetch_all_states

    url_base = "https://exemplo.test/Lista_imoveis_{}.csv"
    requests_mock.get(url_base.format("RO"), status_code=404)

    with pytest.raises(FetchError, match="HTTP 404"):
        fetch_all_states(
            url_base=url_base, input_dir=str(tmp_path), ufs=["RO"], delay=0
        )
