import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
import requests_mock
from pathlib import Path
import os
from unittest.mock import patch, mock_open

# Import functions from fetch_data.py
# Assuming fetch_data.py is in src and tests are in tests/
from fetch_data import (
    _extract_data_for_state,
    _transform_state_data,
    _load_state_data,
    baixar_csvs_por_estado,
    fetch_all_data,
    SCRAPING_COLS,
    OUTPUT_CSV_TEMPLATE,
    BRAZILIAN_STATES
)

# Base URL for mocking
MOCK_URL_BASE = "http://mock-caixa.gov.br/listaweb/Lista_imoveis_{}.htm"

# Sample HTML content for mocking Caixa website responses
SAMPLE_HTML_VALID = """
<html>
<body>
    <table class="responsive sticky-enabled">
        <thead>
            <tr>
                <th>Link Col</th> <!-- Assuming link is not directly here but extracted via XPath -->
                <th>Endereço</th>
                <th>Bairro</th>
                <th>Descrição</th>
                <th>Preço</th>
                <th>Avaliação</th>
                <th>Desconto</th>
                <th>Modalidade</th>
                <th>Foto</th>
                <th>Cidade</th>
                <th>Estado</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td><a href="https://venda-imoveis.caixa.gov.br/sistema/detalhe-imovel.asp?hdnOrigem=index&hdnimovel=12345">Detalhes</a></td>
                <td>Rua Exemplo, 123</td>
                <td>Centro</td>
                <td>Casa com 3 quartos</td>
                <td>R$ 500.000,00</td>
                <td>R$ 480.000,00</td>
                <td>5%</td>
                <td>Venda Direta</td>
                <td>Sim</td>
                <td>Exemplópolis</td>
                <td>EX</td>
            </tr>
            <tr>
                <td><a href="https://venda-imoveis.caixa.gov.br/sistema/detalhe-imovel.asp?hdnOrigem=index&hdnimovel=67890">Detalhes</a></td>
                <td>Av. Principal, 456</td>
                <td>Sul</td>
                <td>Apartamento reformado</td>
                <td>R$ 300.000,00</td>
                <td>R$ 290.000,00</td>
                <td>3.33%</td>
                <td>Leilão</td>
                <td>Não</td>
                <td>Outra Cidade</td>
                <td>EX</td>
            </tr>
        </tbody>
    </table>
</body>
</html>
"""

SAMPLE_HTML_NO_TABLE = "<html><body><p>No table here.</p></body></html>"
SAMPLE_HTML_EMPTY_TABLE = """
<html><body><table class="responsive sticky-enabled"><thead><tr><th>Col1</th></tr></thead><tbody></tbody></table></body></html>
"""

SAMPLE_HTML_DIFFERENT_LINK_STRUCTURE = """
<html><body><table class="responsive sticky-enabled">
<thead><tr><th>Endereço</th><th>Link Details</th></tr></thead>
<tbody><tr><td>Rua Teste</td><td><a href="https://venda-imoveis.caixa.gov.br/sistema/detalhe-imovel.asp?hdnOrigem=index&hdnimovel=999">Link</a></td></tr></tbody>
</table></body></html>
"""


@pytest.fixture
def mock_env_url_base(monkeypatch):
    """Mocks the URL_BASE environment variable."""
    monkeypatch.setenv("URL_BASE", MOCK_URL_BASE)

# Tests for _extract_data_for_state
def test_extract_data_for_state_success(requests_mock):
    state = "EX"
    mock_url = MOCK_URL_BASE.format(state)
    requests_mock.get(mock_url, text=SAMPLE_HTML_VALID)

    df = _extract_data_for_state(state, MOCK_URL_BASE)

    assert not df.empty
    assert len(df) == 2
    assert list(df.columns) == SCRAPING_COLS
    assert df.iloc[0]["link"] == "12345"
    assert df.iloc[0]["endereco"] == "Rua Exemplo, 123"
    assert df.iloc[0]["preco"] == 500000.00
    assert df.iloc[1]["link"] == "67890"

def test_extract_data_for_state_no_url_base_configured():
    df = _extract_data_for_state("EX", None) # Pass None for base_url_fmt_str
    assert df.empty
    assert list(df.columns) == SCRAPING_COLS

    df = _extract_data_for_state("EX", "") # Pass empty string for base_url_fmt_str
    assert df.empty
    assert list(df.columns) == SCRAPING_COLS


def test_extract_data_for_state_http_error(requests_mock):
    state = "EX"
    mock_url = MOCK_URL_BASE.format(state)
    requests_mock.get(mock_url, status_code=500)
    df = _extract_data_for_state(state, MOCK_URL_BASE)
    assert df.empty
    assert list(df.columns) == SCRAPING_COLS

def test_extract_data_for_state_no_table(requests_mock):
    state = "EX"
    mock_url = MOCK_URL_BASE.format(state)
    requests_mock.get(mock_url, text=SAMPLE_HTML_NO_TABLE)
    df = _extract_data_for_state(state, MOCK_URL_BASE)
    assert df.empty
    assert list(df.columns) == SCRAPING_COLS

def test_extract_data_for_state_empty_table(requests_mock):
    state = "EX"
    mock_url = MOCK_URL_BASE.format(state)
    requests_mock.get(mock_url, text=SAMPLE_HTML_EMPTY_TABLE)
    df = _extract_data_for_state(state, MOCK_URL_BASE)
    assert df.empty
    # pd.read_html might return a df with columns even if tbody is empty
    # The function should return an empty DF with SCRAPING_COLS
    assert list(df.columns) == SCRAPING_COLS


# Tests for _transform_state_data
def test_transform_state_data_basic():
    data = {
        "link": ["1", "2", "1"], # Duplicate link
        "endereco": ["Rua A", "Rua B", "Rua A"],
        "bairro": ["centro", "sul", "centro"],
        "cidade": ["CidadeA", "CidadeA", "CidadeA"],
        "estado": ["EX", "EX", "EX"],
        "preco": [100, 200, 100]
    }
    # Ensure all SCRAPING_COLS are present, filling others with defaults
    full_data = {col: data.get(col, [pd.NA] * 3) for col in SCRAPING_COLS}
    df = pd.DataFrame(full_data)

    transformed_df = _transform_state_data(df.copy()) # Pass a copy

    assert len(transformed_df) == 2 # Duplicate removed
    assert transformed_df["bairro"].tolist() == ["CENTRO", "SUL"] # Uppercase and sorted
    # Check sort order (by estado, cidade, link)
    assert transformed_df.iloc[0]["link"] == "1"
    assert transformed_df.iloc[1]["link"] == "2"

def test_transform_state_data_empty_df():
    df = pd.DataFrame(columns=SCRAPING_COLS)
    transformed_df = _transform_state_data(df)
    assert transformed_df.empty

def test_transform_state_data_missing_link_dropna():
    data = {
        "link": [None, "2", "3"],
        "endereco": ["Rua A", "Rua B", "Rua C"],
        "bairro": ["centro", "sul", "norte"],
        "cidade": ["CidadeA", "CidadeA", "CidadeA"],
        "estado": ["EX", "EX", "EX"],
    }
    full_data = {col: data.get(col, [pd.NA] * 3) for col in SCRAPING_COLS}
    df = pd.DataFrame(full_data)
    transformed_df = _transform_state_data(df.copy())
    assert len(transformed_df) == 2
    assert None not in transformed_df["link"].tolist()


# Tests for _load_state_data
@patch("pathlib.Path.mkdir") # Mock mkdir to avoid actual directory creation
@patch("pandas.DataFrame.to_csv") # Mock to_csv to avoid actual file writing
def test_load_state_data_success(mock_to_csv, mock_mkdir):
    state = "EX"
    df_data = {"link": ["1"], "endereco": ["Rua A"], "estado": [state]}
    # Pad with other SCRAPING_COLS
    for col in SCRAPING_COLS:
        if col not in df_data:
            df_data[col] = [pd.NA]

    df = pd.DataFrame(df_data)

    _load_state_data(df, state)

    expected_filepath = Path(OUTPUT_CSV_TEMPLATE.format(state))
    mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
    mock_to_csv.assert_called_once_with(expected_filepath, index=False, encoding='utf-8')

@patch("pandas.DataFrame.to_csv")
def test_load_state_data_empty_df(mock_to_csv):
    state = "EX"
    df = pd.DataFrame(columns=SCRAPING_COLS)
    _load_state_data(df, state)
    mock_to_csv.assert_not_called() # Should not attempt to write if df is empty


# Tests for baixar_csvs_por_estado (integration of extract, transform, load for one state)
@patch("fetch_data._extract_data_for_state")
@patch("fetch_data._transform_state_data")
@patch("fetch_data._load_state_data")
def test_baixar_csvs_por_estado_success(mock_load, mock_transform, mock_extract):
    state = "EX"
    # Mock extract to return a non-empty DataFrame
    mock_df_extracted = pd.DataFrame({"link": ["123"], "estado": [state]})
    # Pad with other SCRAPING_COLS
    for col in SCRAPING_COLS:
        if col not in mock_df_extracted:
            mock_df_extracted[col] = [pd.NA]

    mock_extract.return_value = mock_df_extracted

    # Mock transform to return a (potentially modified) non-empty DataFrame
    mock_df_transformed = pd.DataFrame({"link": ["123"], "estado": [state], "bairro": ["CENTRO"]})
    for col in SCRAPING_COLS:
        if col not in mock_df_transformed:
            mock_df_transformed[col] = [pd.NA]
    mock_transform.return_value = mock_df_transformed

    baixar_csvs_por_estado(state, MOCK_URL_BASE)

    mock_extract.assert_called_once_with(state, MOCK_URL_BASE)
    # assert_frame_equal cannot be directly used on mock call args if df is modified in place by transform
    # So we check that it was called with the result of mock_extract
    # This requires careful handling if the DataFrame object is modified.
    # A simple check is that it's called once.
    assert mock_transform.call_count == 1
    # We can check the type of the first argument to mock_transform
    assert isinstance(mock_transform.call_args[0][0], pd.DataFrame)


    assert mock_load.call_count == 1
    # Check that load was called with the result of transform
    assert isinstance(mock_load.call_args[0][0], pd.DataFrame)
    assert mock_load.call_args[0][1] == state # state argument


@patch("fetch_data._extract_data_for_state")
@patch("fetch_data._transform_state_data")
@patch("fetch_data._load_state_data")
def test_baixar_csvs_por_estado_extract_returns_empty(mock_load, mock_transform, mock_extract):
    state = "EX"
    mock_extract.return_value = pd.DataFrame(columns=SCRAPING_COLS) # Simulate empty extraction

    baixar_csvs_por_estado(state, MOCK_URL_BASE)

    mock_extract.assert_called_once_with(state, MOCK_URL_BASE)
    mock_transform.assert_not_called() # Transform should not be called if extraction is empty
    mock_load.assert_not_called()      # Load should not be called

# Tests for fetch_all_data (main orchestrator)
@patch("fetch_data.baixar_csvs_por_estado")
@patch("fetch_data.load_dotenv") # Mock load_dotenv as it's called inside
@patch.dict(os.environ, {"URL_BASE": MOCK_URL_BASE}, clear=True) # Mock environment variable
@patch("pathlib.Path.mkdir") # Mock directory creation for the seeds root
def test_fetch_all_data_calls_baixar_for_each_state(mock_path_mkdir, mock_load_dotenv, mock_baixar_por_estado):
    fetch_all_data()

    mock_load_dotenv.assert_called_once()
    assert mock_baixar_por_estado.call_count == len(BRAZILIAN_STATES)

    # Check if it was called for a few sample states
    # Get a list of states it was called with
    called_states = [call_args[0][0] for call_args in mock_baixar_por_estado.call_args_list]
    assert "SP" in called_states
    assert "AC" in called_states
    assert "RJ" in called_states

    # Check that the URL_BASE was passed correctly
    for call_args in mock_baixar_por_estado.call_args_list:
        assert call_args[0][1] == MOCK_URL_BASE

    # Check that the seeds directory creation was attempted
    mock_path_mkdir.assert_any_call(parents=True, exist_ok=True)


@patch("fetch_data.baixar_csvs_por_estado")
@patch("fetch_data.load_dotenv")
@patch.dict(os.environ, {}, clear=True) # Simulate URL_BASE not being set
def test_fetch_all_data_no_url_base(mock_load_dotenv, mock_baixar_por_estado):
    fetch_all_data()
    mock_load_dotenv.assert_called_once()
    mock_baixar_por_estado.assert_not_called() # Should not proceed if URL_BASE is missing


@patch("fetch_data.baixar_csvs_por_estado", side_effect=Exception("Test network error"))
@patch("fetch_data.load_dotenv")
@patch.dict(os.environ, {"URL_BASE": MOCK_URL_BASE}, clear=True)
@patch("pathlib.Path.mkdir")
def test_fetch_all_data_exception_in_baixar_csvs_por_estado(mock_path_mkdir, mock_load_dotenv, mock_baixar_por_estado_exception):
    # This test ensures that an exception in processing one state doesn't halt the entire loop
    # (though the current implementation of fetch_all_data might stop on first error if not caught inside loop)
    # The provided fetch_all_data has a try-except inside the loop for baixar_csvs_por_estado.

    fetch_all_data()

    # It should attempt to call for all states, even if some fail
    assert mock_baixar_por_estado_exception.call_count == len(BRAZILIAN_STATES)
    # (The print statement for critical error will be issued for each)

# Example of how to check content of what was "saved" if _load_state_data wasn't fully mocked
# This requires _load_state_data to actually write to a BytesIO or similar if not mocking to_csv
# For now, mocking to_csv is simpler.

# To check print outputs, you can use capsys fixture from pytest:
def test_log_download_success_prints_correctly(capsys):
    from fetch_data import _log_download_success # import locally if needed
    _log_download_success("EX", 10)
    captured = capsys.readouterr()
    assert "[DOWNLOAD] Sucesso para o estado EX: 10 registros baixados." in captured.out

    _log_download_success("NV", 0)
    captured = capsys.readouterr()
    assert "[DOWNLOAD] Aviso para o estado NV: Nenhum registro baixado." in captured.out

# Test that the output path for seeds is correct
def test_output_csv_template_path():
    assert OUTPUT_CSV_TEMPLATE == "dbt_real_estate/seeds/imoveis_{}.csv"
    # Path used in _load_state_data
    state = "TEST"
    expected_path = os.path.join("dbt_real_estate", "seeds", f"imoveis_{state}.csv")
    # Construct path as _load_state_data does
    # output_file_path = Path(OUTPUT_CSV_TEMPLATE.format(state))
    # assert str(output_file_path) == expected_path # Path objects comparison can be tricky
    # Simpler: check the string directly
    assert OUTPUT_CSV_TEMPLATE.format(state) == expected_path

    # Check that parent dir logic in _load_state_data is correct
    # (This is implicitly tested by mock_mkdir in test_load_state_data_success)
    # Path(OUTPUT_CSV_TEMPLATE.format(state)).parent == Path("dbt_real_estate/seeds")

# Consider a test for monetary value conversion in _extract_data_for_state
def test_monetary_conversion_in_extract(requests_mock):
    state = "EX"
    html_monetary = """
    <html><body><table>
    <thead><tr><th>Preço</th><th>Avaliação</th></tr></thead>
    <tbody><tr><td>R$ 1.234,56</td><td>R$ 987.654,32</td></tr></tbody>
    </table></body></html>
    """ # Simplified HTML, assuming other columns are handled/mocked appropriately or not needed for this narrow test

    # Need to make sure the number of columns in HTML matches what read_html expects
    # For this test, let's assume other columns are present and filled with NA or dummy values
    # The key is that "preco" and "avaliacao" are parsed correctly

    # Create a more complete HTML that matches SCRAPING_COLS for pd.read_html
    cols_for_html = SCRAPING_COLS
    html_full_cols_monetary = f"""
    <html><body><table class="responsive sticky-enabled">
    <thead><tr>{''.join(f"<th>{col}</th>" for col in cols_for_html)}</tr></thead>
    <tbody><tr>
        <td><a href="https://venda-imoveis.caixa.gov.br/sistema/detalhe-imovel.asp?hdnOrigem=index&hdnimovel=1">1</a></td>
        {''.join(f"<td>Value for {col}</td>" for col in cols_for_html[1:4])}
        <td>R$ 1.234,56</td>
        <td>R$ 987.654,32</td>
        {''.join(f"<td>Value for {col}</td>" for col in cols_for_html[6:])}
    </tr></tbody>
    </table></body></html>
    """

    mock_url = MOCK_URL_BASE.format(state)
    requests_mock.get(mock_url, text=html_full_cols_monetary)

    df = _extract_data_for_state(state, MOCK_URL_BASE)

    assert not df.empty
    assert df.iloc[0]["preco"] == 1234.56
    assert df.iloc[0]["avaliacao"] == 987654.32

# Test for link extraction robustness (if XPath changes or links are weird)
# (Covered to some extent by existing tests, but could be more specific)
# e.g. test_extract_data_for_state_different_link_structure(requests_mock): ...
# This would require SAMPLE_HTML_DIFFERENT_LINK_STRUCTURE and asserting links are still found if possible, or handled gracefully.
