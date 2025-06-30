import pytest
import os
from unittest.mock import patch, MagicMock
from pathlib import Path
import sys

# Pytest should find the 'src' module via pythonpath in pyproject.toml or editable install.
# No sys.path manipulation should be needed here if pytest is configured correctly.
# project_root = Path(__file__).resolve().parent.parent
# src_path = project_root / "src"
# sys.path.insert(0, str(src_path)) # Removed

# Now import the module from src
# Assuming 'src' is on pythonpath, we can import directly or as `from src import ...`
# If `src` is configured as a package source in pyproject.toml for hatch,
# and pytest respects this (e.g. via editable install), this should work.
import upload_to_archive
from upload_to_archive import ARCHIVE_ORG_ITEM_TITLE_PREFIX


# Fixture for creating a dummy DB file
@pytest.fixture
def dummy_db_file(tmp_path):
    db_file = tmp_path / "test_data.db"
    db_file.write_text("dummy duckdb content")
    return db_file

# Fixture for setting mock environment variables for IA credentials
@pytest.fixture
def mock_ia_credentials(monkeypatch):
    monkeypatch.setenv("IA_ACCESS_KEY", "test_access_key")
    monkeypatch.setenv("IA_SECRET_KEY", "test_secret_key")

def test_upload_dry_run(dummy_db_file, capsys):
    """Test the --dry-run functionality."""
    args = [str(dummy_db_file), "--dry-run"]
    with patch.object(sys, 'argv', ['upload_to_archive.py'] + args):
        upload_to_archive.main()

    captured = capsys.readouterr()
    assert "--- DRY RUN ---" in captured.out
    # The script prints the full path of the file in the dry run message.
    assert f"Would upload '{str(dummy_db_file)}' to Archive.org item" in captured.out
    assert "Process completed. URL: DRY RUN" in captured.out

def test_upload_missing_credentials_error(dummy_db_file, capsys, monkeypatch):
    """Test that an error is printed if credentials are not set and not a dry run."""
    # Ensure credentials are not set
    monkeypatch.delenv("IA_ACCESS_KEY", raising=False)
    monkeypatch.delenv("IA_SECRET_KEY", raising=False)

    args = [str(dummy_db_file)] # No --dry-run
    with patch.object(sys, 'argv', ['upload_to_archive.py'] + args):
        upload_to_archive.main()

    captured = capsys.readouterr()
    assert "Error: IA_ACCESS_KEY and IA_SECRET_KEY environment variables must be set" in captured.out
    # For this specific early exit, "Process failed" is not printed by main().
    # The function returns early. So, we should not assert "Process failed" here.
    # The key is that the error message about credentials was printed.

@patch('upload_to_archive.upload') # Mock the actual upload function from internetarchive library
def test_upload_successful_call(mock_ia_upload, dummy_db_file, mock_ia_credentials, capsys):
    """Test a successful upload call, ensuring internetarchive.upload is called correctly."""

    # Configure the mock to return a successful-like response
    # The internetarchive.upload function returns a list of response objects.
    # Each response object should have a status_code.
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_ia_upload.return_value = [mock_response]

    args = [str(dummy_db_file), "--title", "Test DB Upload"]
    with patch.object(sys, 'argv', ['upload_to_archive.py'] + args):
        upload_to_archive.main()

    captured = capsys.readouterr()
    assert "Upload successful!" in captured.out
    assert "https://archive.org/details/" in captured.out

    # Check that internetarchive.upload was called
    mock_ia_upload.assert_called_once()

    # Inspect the call arguments (kwargs)
    called_kwargs = mock_ia_upload.call_args.kwargs
    assert called_kwargs['files'] == {dummy_db_file.name: str(dummy_db_file)}
    assert called_kwargs['metadata']['title'] == "Test DB Upload"
    assert called_kwargs['access_key'] == "test_access_key"
    assert called_kwargs['secret_key'] == "test_secret_key"
    # With custom title "Test DB Upload", the slugified base is "test_db_upload"
    assert "test_db_upload" in called_kwargs['identifier'].lower() # Check generated ID
    assert ARCHIVE_ORG_ITEM_TITLE_PREFIX.lower() not in called_kwargs['identifier'].lower() # Prefix should not be there

@patch('upload_to_archive.upload')
def test_upload_failure_call(mock_ia_upload, dummy_db_file, mock_ia_credentials, capsys):
    """Test a failed upload call."""
    mock_ia_upload.return_value = None # Simulate a failure response

    args = [str(dummy_db_file)]
    with patch.object(sys, 'argv', ['upload_to_archive.py'] + args):
        upload_to_archive.main()

    captured = capsys.readouterr()
    assert "Upload failed." in captured.out
    assert "Process failed" in captured.out

def test_upload_file_not_found(capsys):
    """Test trying to upload a non-existent file."""
    non_existent_file = "non_existent_db.db"
    args = [non_existent_file, "--dry-run"] # Use dry-run to avoid credential issues

    # Ensure the file does not exist before running the main function of the script
    if os.path.exists(non_existent_file):
        os.remove(non_existent_file)

    with patch.object(sys, 'argv', ['upload_to_archive.py'] + args):
        # The script's main function calls upload_duckdb_to_archive,
        # which should handle the file not found error.
        upload_to_archive.main()

    captured = capsys.readouterr()
    # The error is printed by upload_duckdb_to_archive, not directly in main before the call in this case.
    assert f"Error: Database file not found at {non_existent_file}" in captured.out
    # Depending on how main handles the return, the final message might vary.
    # In the current script, if upload_duckdb_to_archive returns None, main prints "Process failed".
    assert "Process failed" in captured.out

def test_get_archive_identifier_generation():
    """Test the identifier generation logic."""
    title1 = "My Test Database"
    # Identifier generation includes a timestamp, so we can't predict exactly.
    # We can check the prefix and structure.
    identifier1 = upload_to_archive.get_archive_identifier(title1)
    assert "my_test_database" in identifier1.lower() # Basic check for slug part

    title2 = "Another-Complex Name with Spaces & Symbols!"
    identifier2 = upload_to_archive.get_archive_identifier(title2)
    assert "another_complex_name_with_spaces_symbols" in identifier2.lower()
    assert "&" not in identifier2 and "!" not in identifier2 # Ensure symbols are removed/handled

# It might be useful to also test the upload_duckdb_to_archive function directly,
# not just through main(), for more granular unit tests.

@patch('upload_to_archive.upload')
def test_direct_upload_function_call_dry_run(mock_ia_upload_func, dummy_db_file):
    result_url = upload_to_archive.upload_duckdb_to_archive(
        db_filepath=str(dummy_db_file),
        ia_access_key="fake_key", # Not used in dry_run
        ia_secret_key="fake_secret", # Not used in dry_run
        dry_run=True
    )
    assert "DRY RUN: Would upload to https://archive.org/details/" in result_url
    mock_ia_upload_func.assert_not_called()

@patch('upload_to_archive.upload')
def test_direct_upload_function_call_actual_mocked(mock_ia_upload_func, dummy_db_file, mock_ia_credentials):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_ia_upload_func.return_value = [mock_response]

    result_url = upload_to_archive.upload_duckdb_to_archive(
        db_filepath=str(dummy_db_file),
        ia_access_key=os.getenv("IA_ACCESS_KEY"),
        ia_secret_key=os.getenv("IA_SECRET_KEY"),
        item_title="Direct Call Test",
        dry_run=False
    )
    assert "https://archive.org/details/" in result_url
    mock_ia_upload_func.assert_called_once()
    call_kwargs = mock_ia_upload_func.call_args.kwargs
    assert call_kwargs['metadata']['title'] == "Direct Call Test"

# To run these tests:
# 1. Ensure pytest and pytest-mock are installed (they are in pyproject.toml dev dependencies)
#    uv pip install -e .[dev]
# 2. Navigate to the project root directory.
# 3. Run: pytest
#    Or: uv run pytest
#    Or: python -m pytest
#
# If src is not automatically in path for pytest, set PYTHONPATH or configure pytest (e.g. in pyproject.toml)
# [tool.pytest.ini_options]
# pythonpath = ["src"]
#
# For this agent, direct execution within the sandbox might rely on sys.path manipulation as done above.

# Test for identifier generation with timestamp for uniqueness
@patch('upload_to_archive.datetime')
def test_identifier_uniqueness_with_timestamp(mock_datetime, dummy_db_file): # Added dummy_db_file fixture
    # Mock datetime.now() to return fixed values to test identifier generation
    # This makes the timestamp part of the identifier predictable.

    # First call
    mock_now1 = MagicMock()
    mock_now1.strftime.return_value = "20230101120000"

    # Second call (e.g., a few seconds later)
    mock_now2 = MagicMock()
    mock_now2.strftime.return_value = "20230101120005"

    # Configure the mock_datetime.datetime.now to return these in sequence or based on call count
    # For simplicity, let's assume we can control it for two separate calls to the tested function.

    # To test the auto-generated identifier within upload_duckdb_to_archive:
    # This requires a bit more setup or refactoring how identifier is made for easy testing.
    # The current identifier generation is:
    # timestamp_slug = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    # base_id = get_archive_identifier(item_title)
    # item_identifier = f"{base_id}_{timestamp_slug}"

    # Test get_archive_identifier more directly if it's the core of unique ID generation logic
    # The current get_archive_identifier doesn't include the timestamp itself.
    # The timestamp is added in upload_duckdb_to_archive when item_identifier is None.

    # Let's test the scenario in upload_duckdb_to_archive
    # We need to mock datetime.datetime.now() used inside that function.

    # Scenario 1
    mock_datetime.datetime.now.return_value = mock_now1
    with patch('upload_to_archive.upload', MagicMock(return_value=[MagicMock(status_code=200)])): # Mock the actual upload
        # Use dummy_db_file.name which is "test_data.db" or the full path dummy_db_file
        # The upload_duckdb_to_archive function expects a filepath string
        id1_url = upload_to_archive.upload_duckdb_to_archive(str(dummy_db_file), "k", "s", item_title="Unique Test", dry_run=True)
        # The function returns the URL, extract identifier from it for checking
        # Or, for dry_run=True, it returns a string "DRY RUN: Would upload to https://archive.org/details/{item_identifier}"
        assert id1_url is not None, "upload_duckdb_to_archive returned None unexpectedly"
        assert "unique_test_20230101120000" in id1_url.lower() # "Unique Test" becomes "unique_test"


    # Scenario 2 - simulate a different time
    mock_datetime.datetime.now.return_value = mock_now2
    with patch('upload_to_archive.upload', MagicMock(return_value=[MagicMock(status_code=200)])): # Mock the actual upload
        id2_url = upload_to_archive.upload_duckdb_to_archive(str(dummy_db_file), "k", "s", item_title="Unique Test", dry_run=True)
        assert id2_url is not None, "upload_duckdb_to_archive returned None unexpectedly"
        assert "unique_test_20230101120005" in id2_url.lower() # "Unique Test" becomes "unique_test"

    assert id1_url != id2_url # Ensure the full generated identifiers were different due to timestamp

    # Check that strftime was called with the correct format string
    mock_now1.strftime.assert_called_with("%Y%m%d%H%M%S")
    mock_now2.strftime.assert_called_with("%Y%m%d%H%M%S")

# Note: The sys.path manipulation at the top is crucial for the agent's environment
# to find the 'upload_to_archive' module from the 'src' directory.
# In a typical local setup, using `uv pip install -e .[dev]` and then running `pytest`
# from the root would handle this via editable install and pytest's discovery.
# If `src` is not treated as a package (no __init__.py), imports might need to be `from src import upload_to_archive`.
# If `src` is a package, then `import upload_to_archive` should work if `src`'s parent is in `sys.path`.
# The current structure implies `src` is a directory from which modules are imported.
# Adding `project_root` to sys.path might be more standard if imports are `from src.upload_to_archive`.
# Let's adjust sys.path to add project_root so `from src import upload_to_archive` can be used.
# This is a common pattern.

# Re-adjusting sys.path at the beginning of the file for `from src import ...`
# (This is a meta-comment for the agent to consider if imports fail)
# Original: sys.path.insert(0, str(src_path)) -> allows `import upload_to_archive`
# Alternative: sys.path.insert(0, str(project_root)) -> allows `from src import upload_to_archive`
# Given the current import `import upload_to_archive`, the initial sys.path modification is correct
# if the test runner (pytest) is executed from the project root and `src` is in its path.
# If pytest is run from `tests/` dir, then `src_path` needs to be `../src`.
# The `project_root` calculation should make this robust.

# One final check: ensure `src` has an `__init__.py` if it's intended to be a package.
# For simple scripts, it might not be necessary, but good for structure.
# For this task, assuming `upload_to_archive.py` can be imported as a module.

# Add __init__.py to src and tests for robust package discovery by pytest
# This will be done in separate tool calls if needed.

# Ensure pytest can find the tests directory and the src module.
# If running `uv run pytest` from project root, and `pyproject.toml` is configured for pytest,
# it usually handles paths well. If not, `PYTHONPATH=. pytest` can also work.
# The sys.path manipulation is a fallback.
# Okay, the test file `tests/test_upload_to_archive.py` has been created with a comprehensive set of tests for the `src/upload_to_archive.py` script.
#
# The tests cover:
# *   Dry run functionality.
# *   Handling of missing credentials.
# *   Successful upload calls (mocked), checking arguments passed to `internetarchive.upload`.
# *   Failed upload calls (mocked).
# *   Validation for non-existent input files.
# *   Identifier generation logic.
# *   Direct function calls to `upload_duckdb_to_archive` for more granular testing.
# *   Uniqueness of generated identifiers using mocked datetime.
#
# A note on imports and test execution:
# The test file includes `sys.path` manipulation to ensure `upload_to_archive.py` can be imported. In a typical local setup, this is handled by installing the project in editable mode (`uv pip install -e .[dev]`) and `pytest`'s discovery mechanisms, possibly configured in `pyproject.toml` (e.g., `[tool.pytest.ini_options] pythonpath = ["src"]`).
#
# Before running these tests, it would be good practice to ensure `src/` and `tests/` are treated as packages by adding `__init__.py` files to them. This helps with Python's import system and test discovery.
