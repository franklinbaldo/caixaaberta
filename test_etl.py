import pytest
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal
from unittest.mock import MagicMock, patch
import tempfile
import os
from etl import extract_state, transform, load, log_sucess

# Tests will be added here

# Tests for extract_state
def test_extract_state_success(mocker):
    """Test extract_state successfully parses HTML and converts types."""
    mock_response = MagicMock()
    mock_response.text = """
    <html>
        <body>
            <table class="Caixa">
                <tr>
                    <th>Cidade</th>
                    <th>Endereço</th>
                    <th>Bairro</th>
                    <th>Descrição</th>
                    <th>Preço</th>
                    <th>Avaliação</th>
                    <th>Link</th>
                </tr>
                <tr>
                    <td>CIDADE TESTE</td>
                    <td>ENDERECO TESTE, 123</td>
                    <td>BAIRRO TESTE</td>
                    <td>DESCRICAO TESTE</td>
                    <td>R$ 1.000.000,00</td>
                    <td>R$ 2.000.000,00</td>
                    <td><a href="http://example.com/link1">Link</a></td>
                </tr>
                <tr>
                    <td>OUTRA CIDADE</td>
                    <td>RUA EXEMPLO, 456</td>
                    <td>OUTRO BAIRRO</td>
                    <td>OUTRA DESCRICAO</td>
                    <td>R$ 500.000,00</td>
                    <td>R$ 750.000,00</td>
                    <td><a href="http://example.com/link2">Link</a></td>
                </tr>
            </table>
        </body>
    </html>
    """
    mocker.patch('requests.get', return_value=mock_response)

    expected_data = {
        'cidade': ['CIDADE TESTE', 'OUTRA CIDADE'],
        'endereco': ['ENDERECO TESTE, 123', 'RUA EXEMPLO, 456'],
        'bairro': ['BAIRRO TESTE', 'OUTRO BAIRRO'],
        'descricao': ['DESCRICAO TESTE', 'OUTRA DESCRICAO'],
        'preco': [1000000.00, 500000.00],
        'avaliacao': [2000000.00, 750000.00],
        'link': ['http://example.com/link1', 'http://example.com/link2'],
        'estado': ['AC', 'AC']
    }
    expected_df = pd.DataFrame(expected_data)
    # Convert relevant columns to float, as extract_state should do
    expected_df['preco'] = expected_df['preco'].astype(float)
    expected_df['avaliacao'] = expected_df['avaliacao'].astype(float)


    result_df = extract_state('AC')

    # Sort by 'link' for consistent comparison as order might not be guaranteed by parsing
    result_df = result_df.sort_values(by='link').reset_index(drop=True)
    expected_df = expected_df.sort_values(by='link').reset_index(drop=True)


    assert_frame_equal(result_df, expected_df, check_dtype=True)

def test_extract_state_empty_table(mocker):
    """Test extract_state with an empty HTML table."""
    mock_response = MagicMock()
    mock_response.text = """
    <html>
        <body>
            <table class="Caixa">
                <tr>
                    <th>Cidade</th>
                    <th>Endereço</th>
                    <th>Bairro</th>
                    <th>Descrição</th>
                    <th>Preço</th>
                    <th>Avaliação</th>
                    <th>Link</th>
                </tr>
            </table>
        </body>
    </html>
    """
    mocker.patch('requests.get', return_value=mock_response)

    expected_df = pd.DataFrame(columns=[
        'cidade', 'endereco', 'bairro', 'descricao', 'preco', 'avaliacao', 'link', 'estado'
    ])
    # Ensure correct dtypes for empty dataframe
    expected_df['preco'] = expected_df['preco'].astype(float)
    expected_df['avaliacao'] = expected_df['avaliacao'].astype(float)


    result_df = extract_state('DF')
    assert_frame_equal(result_df, expected_df, check_dtype=True)

def test_extract_state_request_error(mocker):
    """Test extract_state when requests.get raises an exception."""
    mocker.patch('requests.get', side_effect=requests.exceptions.RequestException("Test error"))
    with pytest.raises(requests.exceptions.RequestException):
        extract_state('SP')

def test_extract_state_no_table(mocker):
    """Test extract_state when the HTML does not contain the expected table."""
    mock_response = MagicMock()
    mock_response.text = "<html><body><p>No table here.</p></body></html>"
    mocker.patch('requests.get', return_value=mock_response)
    
    # Expect an empty DataFrame or specific error handling if implemented,
    # for now, assuming it returns an empty df if table not found or parsing fails before data extraction
    expected_df = pd.DataFrame(columns=[
        'cidade', 'endereco', 'bairro', 'descricao', 'preco', 'avaliacao', 'link', 'estado'
    ])
    expected_df['preco'] = expected_df['preco'].astype(float)
    expected_df['avaliacao'] = expected_df['avaliacao'].astype(float)

    result_df = extract_state('RJ')
    assert_frame_equal(result_df, expected_df, check_dtype=True)

# Placeholder for importing requests if not already in etl.py, needed for requests.exceptions.RequestException
import requests


# Tests for transform
def test_transform():
    """Test transform function for correct data transformations."""
    data = {
        'estado': ['SP', 'RJ', 'SP', 'MG', 'SP'],
        'cidade': ['SAO PAULO', 'RIO DE JANEIRO', 'SAO PAULO', 'BELO HORIZONTE', 'CAMPINAS'],
        'bairro': ['  BROOKLIN  ', pd.NA, 'BROOKLIN', 'CENTRO', '  CENTRO  '],
        'link': ['link1', 'link2', 'link1', 'link3', 'link4'],
        'preco': [100.0, 200.0, 100.0, 150.0, 120.0], # Added for sort order
        'avaliacao': [100.0, 200.0, 100.0, 150.0, 120.0] # Added for sort order
    }
    input_df = pd.DataFrame(data)

    expected_data = {
        'estado': ['MG', 'RJ', 'SP', 'SP'],
        'cidade': ['BELO HORIZONTE', 'RIO DE JANEIRO', 'CAMPINAS', 'SAO PAULO'],
        'bairro': ['CENTRO', '', 'CENTRO', 'BROOKLIN'],
        'link': ['link3', 'link2', 'link4', 'link1'],
        'preco': [150.0, 200.0, 120.0, 100.0],
        'avaliacao': [150.0, 200.0, 120.0, 100.0]
    }
    expected_df = pd.DataFrame(expected_data)

    transformed_df = transform(input_df.copy()) # Use .copy() to avoid modifying input_df in place if transform does so

    # Sort both dataframes by all columns to ensure comparison is order-independent for non-specified sort columns
    # The function sorts by ['estado', 'cidade', 'bairro', 'preco', 'avaliacao']
    # Duplicates are dropped based on ['estado', 'cidade', 'link'] prior to this sort
    
    # To properly test, we should assert the state *after* drop_duplicates and *after* sort
    # The expected_df is already in the final sorted order and with duplicates dropped.

    assert_frame_equal(transformed_df.reset_index(drop=True), expected_df.reset_index(drop=True), check_dtype=False)

def test_transform_empty_dataframe():
    """Test transform with an empty DataFrame."""
    input_df = pd.DataFrame(columns=['estado', 'cidade', 'bairro', 'link', 'preco', 'avaliacao'])
    expected_df = pd.DataFrame(columns=['estado', 'cidade', 'bairro', 'link', 'preco', 'avaliacao'])
    
    # Ensure dtypes match for empty dataframes, especially for object columns like 'bairro'
    expected_df['bairro'] = expected_df['bairro'].astype(object)


    transformed_df = transform(input_df.copy())
    assert_frame_equal(transformed_df, expected_df, check_dtype=True)

def test_transform_bairro_stripping_and_uppercase():
    """Test that bairro is correctly stripped and uppercased."""
    data = {
        'estado': ['SP'],
        'cidade': ['SAO PAULO'],
        'bairro': ['  vila olímpia  '], # leading/trailing spaces, lowercase
        'link': ['link1'],
        'preco': [100.0],
        'avaliacao': [100.0]
    }
    input_df = pd.DataFrame(data)
    transformed_df = transform(input_df.copy())
    assert transformed_df['bairro'].iloc[0] == 'VILA OLÍMPIA'

def test_transform_bairro_na_fill():
    """Test that NA values in bairro are filled with empty string."""
    data = {
        'estado': ['SP'],
        'cidade': ['SAO PAULO'],
        'bairro': [pd.NA],
        'link': ['link1'],
        'preco': [100.0],
        'avaliacao': [100.0]
    }
    input_df = pd.DataFrame(data)
    transformed_df = transform(input_df.copy())
    assert transformed_df['bairro'].iloc[0] == ''


# Tests for load
def test_load():
    """Test load function writes DataFrame to CSV."""
    sample_data = {
        'col1': [1, 2],
        'col2': ['a', 'b']
    }
    sample_df = pd.DataFrame(sample_data)

    # Create a temporary file to write the CSV
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv', newline='') as tmpfile:
        temp_file_name = tmpfile.name
    
    try:
        # Call the load function
        load(sample_df, temp_file_name)

        # Read the CSV back and assert its content
        loaded_df = pd.read_csv(temp_file_name)
        assert_frame_equal(loaded_df, sample_df, check_dtype=False) # dtypes can sometimes differ after CSV write/read for simple types
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_file_name):
            os.remove(temp_file_name)

def test_load_empty_dataframe():
    """Test load function with an empty DataFrame."""
    empty_df = pd.DataFrame({'colA': pd.Series(dtype='int'), 'colB': pd.Series(dtype='str')})

    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv', newline='') as tmpfile:
        temp_file_name = tmpfile.name

    try:
        load(empty_df, temp_file_name)
        loaded_df = pd.read_csv(temp_file_name)
        
        # For empty dataframes, pandas by default reads all columns as object if no data,
        # or might infer differently than when dataframe was created.
        # We need to ensure the columns are the same, and data is empty.
        # `assert_frame_equal` can be strict about dtypes.
        # One way is to check columns and shape, and then ensure it's empty.
        pd.testing.assert_index_equal(loaded_df.columns, empty_df.columns)
        assert loaded_df.empty
        # If specific dtypes are crucial even for empty CSV, ensure original dtypes are written and read back.
        # However, CSV is a text format and doesn't preserve pandas dtypes perfectly for empty files.
        # A common approach is to check column names and that the DataFrame is empty.
        # For this test, let's ensure the column names are correct and the dataframe is empty.
        # If the `load` function itself has specific dtype handling for empty DFs, adjust here.
        # Assuming standard to_csv, this should be fine.
        # assert_frame_equal for empty dataframes can be tricky with dtypes.
        # Checking columns and emptiness is usually sufficient.
        # If dtypes are critical, ensure they are written/read consistently or convert after reading.
        # For this case, check_dtype=False is acceptable if data types are not strictly enforced for empty CSVs by the requirements.
        # Let's keep check_dtype=False as per the original test_load.
        assert_frame_equal(loaded_df, empty_df, check_dtype=False)


    finally:
        if os.path.exists(temp_file_name):
            os.remove(temp_file_name)


# Tests for log_sucess
# If etl.py uses `import datetime` and calls `datetime.datetime.now()`
# then we need to patch `datetime.datetime` within the etl module's scope (which has `import datetime`).
@patch('etl.datetime.datetime') 
def test_log_sucess(mock_datetime_class): # mock_datetime_class is now a mock of the datetime class
    """Test log_sucess appends correctly formatted log message."""
    
    from datetime import datetime # Import for creating a real datetime object
    fixed_now_datetime = datetime(2024, 1, 1, 12, 0, 0)
    # Configure the .now() classmethod of the mocked datetime class
    mock_datetime_class.now.return_value = fixed_now_datetime

    sample_data = {'col1': [1, 2]}
    sample_df = pd.DataFrame(sample_data)
    estado_teste = "TT"

    # Create a temporary file for the log
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.log') as tmpfile:
        log_file_name = tmpfile.name
    
    original_log_file_path = 'etl.log' # Path to the log file in etl.py
    
    try:
        # Call log_sucess with the correctly configured mock
        # Ensure file is clear before this call by virtue of NamedTemporaryFile mode 'w+'
        # or explicitly clear if reusing across calls (not the case here for a single call).
        with patch('etl.log', log_file_name):
             log_sucess(estado_teste, sample_df) 

        with open(log_file_name, 'r') as f:
            log_content = f.read().strip()

        # pandas to_csv for a DataFrame with a datetime column will use its string representation.
        # Example: "2024-01-01 12:00:00"
        expected_log_entry = f"{estado_teste},{str(fixed_now_datetime)}"
        assert log_content == expected_log_entry

    finally:
        # Clean up the temporary log file
        if os.path.exists(log_file_name):
            os.remove(log_file_name)

def test_log_sucess_empty_dataframe():
    """Test log_sucess raises AssertionError for empty DataFrame."""
    empty_df = pd.DataFrame()
    estado_teste = "EE"
    # Corrected argument order and assertion message
    with pytest.raises(AssertionError, match="Empty dataframe"): 
        log_sucess(estado_teste, empty_df)

# It seems there was an issue with the previous datetime mock.
# `etl.datetime` implies that `datetime` is an attribute or import within the `etl` module.
# If `datetime` is imported as `from datetime import datetime`, then the patch target is different.
# Assuming `import datetime` or `from datetime import datetime as dt_alias` in etl.py
# If `etl.py` uses `from datetime import datetime`, then `@patch('etl.datetime')` is correct if `etl` module has `datetime = datetime`.
# If `etl.py` has `import datetime`, then `@patch('etl.datetime.datetime')` might be needed if `datetime.now()` is called.
# The provided solution uses `@patch('etl.datetime')` which suggests `datetime` is directly available in `etl`'s namespace.
# Let's ensure `etl.py` uses `datetime` in a way that `@patch('etl.datetime')` is effective.
# If `etl.py` has `from datetime import datetime`, then `log_sucess` calls `datetime.now()`.
# In this case, the patch should be `@patch('etl.datetime')` if `etl.py` contains `import datetime` and calls `datetime.datetime.now()`
# or if `etl.py` contains `from datetime import datetime` and calls `datetime.now()`, the patch should target where `datetime` is looked up.
# The current patch `etl.datetime` implies that `etl.py` might have `import datetime as datetime` or `datetime = __import__('datetime')`.
# Let's assume `from datetime import datetime` is used in `etl.py`, so `datetime.now()` is called.
# The patch should be `@patch('etl.datetime')` if `datetime` is an object within `etl` that has a `now` method.
# If `etl.py` does:
# ```python
# from datetime import datetime
# def log_sucess(...):
#    now = datetime.now()
# ```
# Then the correct patch is `@patch('etl.datetime')`.

# Re-checking the previous thought:
# If `etl.py` has `from datetime import datetime`, then `log_sucess` calls `datetime.now()`.
# In this case, `datetime` is a class, and `now` is a class method.
# So `@patch('etl.datetime.now')` would be more precise if `datetime` refers to the class.
# However, if `etl.py` has `import datetime` and then calls `datetime.datetime.now()`,
# then `@patch('etl.datetime.datetime')` would be the target for mocking the `datetime` class within the `datetime` module.

# Let's assume `etl.py` uses `from datetime import datetime`.
# The call is `datetime.now()`.
# So we need to mock the `datetime` object in `etl.py`.
# `@patch('etl.datetime')` should work.
# The previous `test_log_sucess` uses this, let's stick to it and verify during execution.
# The key is that `mock_datetime` passed to the test is the mock for `etl.datetime`.
# So `mock_datetime.now.return_value` configures the `now` method of this mocked `datetime` object.
