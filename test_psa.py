import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
from pathlib import Path
import tempfile
from datetime import datetime, date
from unittest.mock import patch, MagicMock # Import MagicMock
from etl import cols as etl_cols # Import etl_cols from etl.py
from psa import get_last_etl, update_records, cols as psa_cols, sorting_cols, log as psa_log_file_name

# Define etl_cols if not imported or if you want a specific version for tests
# For this test, we rely on the imported etl_cols from etl.py:
# etl_cols = ['link', 'endereco', 'bairro', 'descricao', 'preco', 'avaliacao', 'desconto', 'modalidade', 'foto', 'cidade', 'estado']


@pytest.fixture
def temp_data_dir(tmp_path):
    """Create a temporary data directory like 'data/'."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    return data_dir

@pytest.fixture
def temp_log_file(tmp_path):
    """Create a temporary log file path."""
    return tmp_path / psa_log_file_name # Use the log file name from psa.py

# --- Tests for get_last_etl ---

def test_get_last_etl_success(temp_log_file):
    log_content = (
        "AC,2024-01-15 10:00:00.123456\n"
        "SP,2024-01-14 12:00:00.000000\n"
        "AC,2024-01-16 09:30:00.789012\n" # AC updated
        "RJ,2024-01-15 11:00:00.000000\n"
    )
    temp_log_file.write_text(log_content)
    
    # Patch psa.log to use our temporary log file path
    with patch('psa.log', str(temp_log_file)):
        last_etl_times = get_last_etl()

    assert isinstance(last_etl_times, dict)
    assert len(last_etl_times) == 3
    assert last_etl_times['AC'] == pd.Timestamp('2024-01-16 09:30:00.789012')
    assert last_etl_times['SP'] == pd.Timestamp('2024-01-14 12:00:00')
    assert last_etl_times['RJ'] == pd.Timestamp('2024-01-15 11:00:00')

def test_get_last_etl_empty_file(temp_log_file):
    temp_log_file.write_text("") # Empty file
    with patch('psa.log', str(temp_log_file)):
        # Expecting an error or specific handling. pandas read_csv on empty file with header=None might error.
        # The function uses read_csv with header=None, names=["state", "date"]
        # This will likely raise an EmptyDataError from pandas if the file is truly empty.
        # If it has content but cannot be parsed, it might raise other errors.
        # For a file that's just empty, it usually means no data.
        try:
            last_etl_times = get_last_etl()
            assert last_etl_times == {} # Or expect specific error and catch it.
        except pd.errors.EmptyDataError: # If pandas raises this for an empty file
             assert get_last_etl() == {} # Or handle as desired

def test_get_last_etl_file_not_found(tmp_path):
    # Test with a non-existent log file path
    # Ensure the patched path does not exist
    non_existent_log_path = tmp_path / "non_existent_etl.log"
    with patch('psa.log', str(non_existent_log_path)):
        # The function itself doesn't have a try-except for FileNotFoundError for pd.read_csv
        # So, we expect pd.read_csv to raise it.
        with pytest.raises(FileNotFoundError):
            get_last_etl()

def test_get_last_etl_malformed_date(temp_log_file):
    log_content = "AC,NOT_A_DATE\nSP,2024-01-14 12:00:00\n"
    temp_log_file.write_text(log_content)
    with patch('psa.log', str(temp_log_file)):
        # pandas to_datetime with infer_datetime_format=True might parse "NOT_A_DATE" as NaT
        last_etl_times = get_last_etl()
    assert pd.isna(last_etl_times.get('AC')) # Should be NaT
    assert last_etl_times['SP'] == pd.Timestamp('2024-01-14 12:00:00')


# --- Tests for update_records ---

TS_OLD_STR = "2023-01-01"
TS_NOW_STR = "2023-02-01"
TS_UPDATE_STR = "2023-03-01"

TS_OLD_DATE = date(2023, 1, 1)
TS_NOW_DATE = date(2023, 2, 1)
TS_UPDATE_DATE = date(2023, 3, 1)


@pytest.fixture
def mock_timestamp_now(mocker):
    mock_dt = datetime.strptime(TS_NOW_STR, '%Y-%m-%d')
    return mocker.patch('pandas.Timestamp.now', return_value=pd.Timestamp(mock_dt))

@pytest.fixture
def mock_timestamp_update(mocker):
    mock_dt = datetime.strptime(TS_UPDATE_STR, '%Y-%m-%d')
    return mocker.patch('pandas.Timestamp.now', return_value=pd.Timestamp(mock_dt))


def create_dummy_state_csv(data_dir, state_code, data, header_cols):
    file_path = data_dir / f"imoveis_{state_code}.csv"
    df = pd.DataFrame(data, columns=header_cols)
    df.to_csv(file_path, index=False)
    return file_path

# Use etl_cols for dummy state CSVs
dummy_data_sp1 = [
    ["link_sp1", "addr_sp1", "bairro_sp1", "desc_sp1", 100.0, 110.0, 0.1, "mod_sp1", "foto_sp1", "cidade_sp1", "SP"],
]
dummy_data_rj1 = [
    ["link_rj1", "addr_rj1", "bairro_rj1", "desc_rj1", 200.0, 220.0, 0.2, "mod_rj1", "foto_rj1", "cidade_rj1", "RJ"],
]


def test_update_records_initial_run(temp_data_dir, tmp_path, mock_timestamp_now):
    output_csv = tmp_path / "test_imoveis_BR.csv"
    create_dummy_state_csv(temp_data_dir, "SP", dummy_data_sp1, etl_cols)
    create_dummy_state_csv(temp_data_dir, "RJ", dummy_data_rj1, etl_cols)

    # Store the true original Path.glob method before any patches in this test
    true_original_glob = Path.glob

    def new_mock_glob(self_path_instance, pattern):
        # self_path_instance is the Path object .glob() is called on.
        # Compare resolved paths to ensure robustness
        expected_data_path_resolved = Path("data").resolve()
        # print(f"DEBUG MOCK GLOB: self_path_instance='{self_path_instance}', resolved='{self_path_instance.resolve()}', expected_resolved='{expected_data_path_resolved}'")
        if self_path_instance.resolve() == expected_data_path_resolved:
            print(f"MOCK GLOB for Path('data/'): Looking in {temp_data_dir} for {pattern}")
            # Use the true original glob on temp_data_dir
            result = list(true_original_glob(temp_data_dir, pattern))
            print(f"MOCK GLOB for Path('data/'): Found files: {result}")
            return iter(result)
        else:
            # For all other Path objects, use the true original glob
            # print(f"MOCK GLOB for {self_path_instance} (unmocked part): Using original glob.")
            return true_original_glob(self_path_instance, pattern)

    # Patch pathlib.Path.glob globally for the duration of this context
    with patch.object(Path, 'glob', new_mock_glob):
        update_records(output=str(output_csv))

    assert output_csv.exists()
    df_br = pd.read_csv(output_csv)

    assert len(df_br) == 2 # SP1 and RJ1
    assert 'first_time_seen' in df_br.columns
    assert 'not_seen_since' in df_br.columns
    
    # Check first_time_seen (should be date part of TS_NOW_STR)
    # Convert to string to avoid timezone issues if any, then compare date part
    assert pd.to_datetime(df_br['first_time_seen'].iloc[0]).strftime('%Y-%m-%d') == TS_NOW_STR
    assert pd.to_datetime(df_br['first_time_seen'].iloc[1]).strftime('%Y-%m-%d') == TS_NOW_STR
    
    # not_seen_since should be NaN (represented as NaT for datetime, or float NaN if column is mixed)
    assert pd.isna(df_br['not_seen_since'].iloc[0])
    assert pd.isna(df_br['not_seen_since'].iloc[1])
    assert "dataset" not in df_br.columns # Helper column should be dropped

    # Check sorting (example, assuming 'estado' is primary sort key)
    # This depends on psa.sorting_cols. For simplicity, check one aspect.
    # Example: RJ should come after SP if sorting by 'estado' descending, or check full sort
    # This requires knowing sorting_cols and their order. Let's assume it's sorted.
    # For now, a simple check on content based on known inputs
    assert "link_sp1" in df_br["link"].values
    assert "link_rj1" in df_br["link"].values


def test_update_records_update_run(temp_data_dir, tmp_path, mock_timestamp_update):
    output_csv = tmp_path / "test_imoveis_BR.csv"

    # 1. Create initial imoveis_BR.csv (history)
    # Columns for history: etl_cols + ["first_time_seen", "not_seen_since"] which are in psa_cols
    history_data = [
        # Record that will persist
        ["link_sp1", "addr_sp1", "bairro_sp1", "desc_sp1", 100.0, 110.0, 0.1, "mod_sp1", "foto_sp1", "cidade_sp1", "SP", TS_OLD_STR, None],
        # Record that will be marked as not_seen_since
        ["link_old_absent", "addr_old", "b_old", "d_old", 50.0, 55.0, 0.05, "mod_old", "foto_old", "c_old", "XX", TS_OLD_STR, None]
    ]
    df_history_initial = pd.DataFrame(history_data, columns=psa_cols) # Use psa_cols
    # Ensure correct date types for writing to CSV
    df_history_initial['first_time_seen'] = pd.to_datetime(df_history_initial['first_time_seen']).dt.date
    df_history_initial['not_seen_since'] = pd.to_datetime(df_history_initial['not_seen_since']).dt.date
    df_history_initial.to_csv(output_csv, index=False)

    # 2. Create new state CSVs for the update
    # SP record is the same, RJ record is new
    dummy_data_sp_update = dummy_data_sp1 # Same as before
    dummy_data_rj_new = [
        ["link_rj_new", "addr_rj_new", "b_rj_new", "d_rj_new", 250.0, 275.0, 0.3, "mod_rj_new", "foto_rj_new", "c_rj_new", "RJ"],
    ]
    create_dummy_state_csv(temp_data_dir, "SP", dummy_data_sp_update, etl_cols)
    create_dummy_state_csv(temp_data_dir, "RJ", dummy_data_rj_new, etl_cols) # RJ has a new record

    # Store the true original Path.glob method before any patches in this test
    true_original_glob_update = Path.glob

    def new_mock_glob_update_run(self_path_instance, pattern):
        expected_data_path_resolved = Path("data").resolve()
        # print(f"DEBUG MOCK GLOB UPDATE: self_path_instance='{self_path_instance}', resolved='{self_path_instance.resolve()}', expected_resolved='{expected_data_path_resolved}'")
        if self_path_instance.resolve() == expected_data_path_resolved:
            print(f"MOCK GLOB UPDATE for Path('data/'): Looking in {temp_data_dir} for {pattern}")
            result = list(true_original_glob_update(temp_data_dir, pattern))
            print(f"MOCK GLOB UPDATE for Path('data/'): Found files: {result}")
            return iter(result)
        else:
            # print(f"MOCK GLOB UPDATE for {self_path_instance} (unmocked part): Using original glob.")
            return true_original_glob_update(self_path_instance, pattern)

    # Patch pathlib.Path.glob globally for the duration of this context
    with patch.object(Path, 'glob', new_mock_glob_update_run):
        update_records(output=str(output_csv))

    df_br_updated = pd.read_csv(output_csv)
    
    assert len(df_br_updated) == 3 # sp1 (persisted), old_absent (marked), rj_new (new)

    record_sp1 = df_br_updated[df_br_updated["link"] == "link_sp1"].iloc[0]
    record_old_absent = df_br_updated[df_br_updated["link"] == "link_old_absent"].iloc[0]
    record_rj_new = df_br_updated[df_br_updated["link"] == "link_rj_new"].iloc[0]

    # Existing record (sp1): first_time_seen is old, not_seen_since is NaN
    assert pd.to_datetime(record_sp1['first_time_seen']).date() == TS_OLD_DATE
    assert pd.isna(record_sp1['not_seen_since'])

    # Old record (old_absent): first_time_seen is old, not_seen_since is TS_UPDATE_DATE
    assert pd.to_datetime(record_old_absent['first_time_seen']).date() == TS_OLD_DATE
    assert pd.to_datetime(record_old_absent['not_seen_since']).date() == TS_UPDATE_DATE
    
    # New record (rj_new): first_time_seen is TS_UPDATE_DATE, not_seen_since is NaN
    assert pd.to_datetime(record_rj_new['first_time_seen']).date() == TS_UPDATE_DATE
    assert pd.isna(record_rj_new['not_seen_since'])
    
    assert "dataset" not in df_br_updated.columns
