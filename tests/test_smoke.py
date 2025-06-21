import pytest
import subprocess
import sys
from pathlib import Path
import os
import shutil

# Define project root assuming tests/ is at the project root
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
TEST_OUTPUT_CSV = PROJECT_ROOT / "imoveis_BR_smoke_test.csv"
TEST_CACHE_SQLITE = DATA_DIR / "cache_smoke_test.sqlite"

# Original files to be backed up and restored if they exist
ORIGINAL_CSV = PROJECT_ROOT / "imoveis_BR.csv"
ORIGINAL_CACHE = DATA_DIR / "cache.sqlite" # As per new cache.py location

# Backup names
BACKUP_CSV = PROJECT_ROOT / "imoveis_BR.csv.backup"
BACKUP_CACHE = DATA_DIR / "cache.sqlite.backup"


@pytest.fixture(scope="module", autouse=True)
def backup_and_cleanup_data():
    print("SMOKE TEST FIXTURE: Setup started.")
    try:
        # Backup original files if they exist
        if ORIGINAL_CSV.exists():
            print(f"SMOKE TEST FIXTURE: Backing up {ORIGINAL_CSV} to {BACKUP_CSV}")
            shutil.copy(ORIGINAL_CSV, BACKUP_CSV)
        if ORIGINAL_CACHE.exists():
            print(f"SMOKE TEST FIXTURE: Backing up {ORIGINAL_CACHE} to {BACKUP_CACHE}")
            shutil.copy(ORIGINAL_CACHE, BACKUP_CACHE)

        # Ensure test output files from previous runs (if any) are clean.
        # The pipeline writes to ORIGINAL_CSV and ORIGINAL_CACHE by default.
        # So, we ensure these are removed before each test *module* run.
        print(f"SMOKE TEST FIXTURE: Ensuring {ORIGINAL_CSV} is removed before test.")
        ORIGINAL_CSV.unlink(missing_ok=True)
        print(f"SMOKE TEST FIXTURE: Ensuring {ORIGINAL_CACHE} is removed before test.")
        ORIGINAL_CACHE.unlink(missing_ok=True)

        # Clean any state CSVs in data/ directory
        print(f"SMOKE TEST FIXTURE: Cleaning state CSVs from {DATA_DIR}")
        for f_path in DATA_DIR.glob("imoveis_*.csv"):
            if f_path.name != "imoveis_BR.csv": # Should not happen if DATA_DIR is just data/
                print(f"SMOKE TEST FIXTURE: Removing {f_path}")
                f_path.unlink(missing_ok=True)

    except Exception as e:
        print(f"SMOKE TEST FIXTURE: Error during setup: {e}")
        pytest.fail(f"Fixture setup error: {e}")

    yield # Run the tests

    print("SMOKE TEST FIXTURE: Teardown started.")
    try:
        # Cleanup files potentially created by the test run at default locations
        print(f"SMOKE TEST FIXTURE: Cleaning up {ORIGINAL_CSV} after test.")
        ORIGINAL_CSV.unlink(missing_ok=True)
        print(f"SMOKE TEST FIXTURE: Cleaning up {ORIGINAL_CACHE} after test.")
        ORIGINAL_CACHE.unlink(missing_ok=True)

        # Restore original files from backup
        if BACKUP_CSV.exists():
            print(f"SMOKE TEST FIXTURE: Restoring {ORIGINAL_CSV} from {BACKUP_CSV}")
            shutil.move(BACKUP_CSV, ORIGINAL_CSV)
        if BACKUP_CACHE.exists():
            print(f"SMOKE TEST FIXTURE: Restoring {ORIGINAL_CACHE} from {BACKUP_CACHE}")
            shutil.move(BACKUP_CACHE, ORIGINAL_CACHE)
        print("SMOKE TEST FIXTURE: Teardown finished.")
    except Exception as e:
        print(f"SMOKE TEST FIXTURE: Error during teardown: {e}")
        # Don't fail the test itself for teardown errors, but log them.


def run_pipeline_as_module(geo_enabled=False, skip_download=True):
    """Runs the pipeline as a module: python -m caixaaberta.pipeline"""
    command = [sys.executable, "-m", "caixaaberta.pipeline"]
    if geo_enabled:
        command.append("--geo")
    if skip_download:
        command.append("--skip-download")

    # env = os.environ.copy()
    # env["PYTHONPATH"] = str(PROJECT_ROOT / "src") + os.pathsep + env.get("PYTHONPATH", "") # No longer needed if package is installed

    try:
        # Using PROJECT_ROOT as cwd ensures that relative paths like "data/" in pipeline.py work as expected.
        print(f"SMOKE TEST: Running command: {' '.join(command)}") # Removed PYTHONPATH from log
        result = subprocess.run(command, capture_output=True, text=True, check=False, cwd=PROJECT_ROOT, timeout=300) # env=env removed
        print("SMOKE TEST STDOUT:")
        print(result.stdout)
        print("SMOKE TEST STDERR:")
        print(result.stderr)
        result.check_returncode() # Raise CalledProcessError if return code is non-zero
        return True
    except subprocess.CalledProcessError as e:
        print(f"SMOKE TEST: Pipeline execution failed with return code {e.returncode}")
        return False
    except subprocess.TimeoutExpired:
        print("SMOKE TEST: Pipeline execution timed out.")
        return False
    except Exception as e:
        print(f"SMOKE TEST: An unexpected error occurred during pipeline execution: {e}")
        return False

def test_pipeline_runs_without_error_no_geo():
    """Test that the pipeline runs without error (no geocoding)."""
    # Pre-cleanup specific files that this test might generate at default locations
    if ORIGINAL_CSV.exists(): ORIGINAL_CSV.unlink()
    if ORIGINAL_CACHE.exists(): ORIGINAL_CACHE.unlink() # Cache is in data/

    assert run_pipeline_as_module(geo_enabled=False, skip_download=True), "Pipeline failed to run without geocoding."
    # Check if the main output CSV is created
    assert ORIGINAL_CSV.exists(), f"Output CSV {ORIGINAL_CSV} was not created."
    # Cache should not be created or should be empty if --geo is not used
    # Depending on cache.py, it might always create an empty DB file.
    # For this smoke test, we primarily care that it ran.
    # If cache is always created by cache.py import, then check it's small or empty.
    if ORIGINAL_CACHE.exists():
        # Basic check: file exists. More specific checks could be added.
        pass


def test_pipeline_runs_with_geo_enabled():
    """Test that the pipeline runs with geocoding enabled."""
    # Pre-cleanup specific files
    if ORIGINAL_CSV.exists(): ORIGINAL_CSV.unlink()
    if ORIGINAL_CACHE.exists(): ORIGINAL_CACHE.unlink()

    # This test requires a GEOCODER_KEY in .env for actual geocoding to be tested.
    # If not present, it will run but geocoding attempts might just return None.
    # The test ensures the pipeline doesn't crash with --geo flag.
    geocoder_key = os.getenv("GEOCODER_KEY")
    if not geocoder_key:
        print("Warning: GEOCODER_KEY not set. Geocoding functionality will be limited but pipeline should still run.")

    assert run_pipeline_as_module(geo_enabled=True, skip_download=True), "Pipeline failed to run with geocoding enabled."
    assert ORIGINAL_CSV.exists(), f"Output CSV {ORIGINAL_CSV} was not created with --geo."
    # With --geo, cache.sqlite should be created (and potentially populated if GEOCODER_KEY is valid and there are addresses to geocode)
    assert ORIGINAL_CACHE.exists(), f"Cache file {ORIGINAL_CACHE} was not created with --geo."

# To make it runnable with `python tests/test_smoke.py` for debugging locally
if __name__ == "__main__":
    pytest.main([__file__])
