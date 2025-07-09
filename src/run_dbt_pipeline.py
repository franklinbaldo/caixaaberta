import subprocess
import argparse
import sys
from pathlib import Path

# Define the root of the project and dbt project path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_real_estate"
DB_FILENAME = "real_estate_data.db" # As defined in profiles.yml
DUCKDB_FILE_PATH = DBT_PROJECT_DIR / DB_FILENAME
VENV_DBT_EXECUTABLE = PROJECT_ROOT / ".venv" / "bin" / "dbt"
UPLOAD_SCRIPT_PATH = PROJECT_ROOT / "src" / "upload_to_archive.py"

def run_command(command, cwd=None, shell=False):
    """Helper function to run a shell command and print its output."""
    print(f"Running command: {' '.join(command)}")
    try:
        process = subprocess.Popen(command, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, shell=shell)
        for line in process.stdout:
            print(line, end='')
        process.wait()
        if process.returncode != 0:
            print(f"Error: Command '{' '.join(command)}' failed with exit code {process.returncode}")
            return False
    except Exception as e:
        print(f"An exception occurred while running command '{' '.join(command)}': {e}")
        return False
    return True

def main():
    parser = argparse.ArgumentParser(description="Run the dbt pipeline: build DuckDB and upload to Archive.org.")
    parser.add_argument(
        "--skip-dbt-build",
        action="store_true",
        help="Skip the 'dbt build' step. Useful if the database is already built."
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip the Archive.org upload step."
    )
    parser.add_argument(
        "--upload-dry-run",
        action="store_true",
        help="Perform a dry run for the Archive.org upload (no actual upload)."
    )
    parser.add_argument(
        "--archive-item-identifier",
        help="Optional: Specific Archive.org item identifier for the upload."
    )
    parser.add_argument(
        "--archive-item-title",
        help="Optional: Title for the Archive.org item for the upload."
    )
    parser.add_argument(
        "--archive-item-description",
        help="Optional: Description for the Archive.org item for the upload."
    )
    args = parser.parse_args()

    # Step 1: Run dbt build (or seed, then test, then run for models if more granular)
    if not args.skip_dbt_build:
        print("\n--- Running dbt build ---")
        # dbt commands should be run from within the dbt project directory
        # or specify --project-dir. We also need --profiles-dir.
        dbt_command = [
            str(VENV_DBT_EXECUTABLE),
            "build",
            "--project-dir", str(DBT_PROJECT_DIR),
            "--profiles-dir", str(DBT_PROJECT_DIR) # Point to profiles.yml in dbt_real_estate
        ]
        # To run dbt tests for seeds, you'd typically define tests in schema.yml files
        # `dbt build` includes running tests if they are defined.
        # If only seeds are present and no tests are defined for them yet,
        # `dbt seed` followed by `dbt test` (if tests exist) would be another option.
        # `dbt build` is more comprehensive for future models.

        if not run_command(dbt_command):
            print("dbt build failed. Aborting pipeline.")
            sys.exit(1)
        print("--- dbt build completed successfully. ---")
    else:
        print("Skipping dbt build step as per --skip-dbt-build.")

    # Verify the DuckDB file exists
    if not DUCKDB_FILE_PATH.exists():
        print(f"Error: DuckDB file '{DUCKDB_FILE_PATH}' not found after dbt build (or if build was skipped).")
        print("Ensure dbt build ran correctly or the file exists if skipping build.")
        if not args.skip_dbt_build: # Only exit if we actually tried to build it and it failed to appear
            sys.exit(1)
        elif not args.skip_upload: # If we intended to upload but the file is missing
             print("Cannot proceed with upload if DB file is missing and build was skipped.")
             sys.exit(1)


    # Step 2: Upload to Archive.org
    if not args.skip_upload:
        if not DUCKDB_FILE_PATH.exists():
            print(f"Error: DuckDB file '{DUCKDB_FILE_PATH}' does not exist. Cannot upload.")
            sys.exit(1)

        print("\n--- Uploading to Archive.org ---")
        upload_command = [
            sys.executable, # Use the current Python interpreter
            str(UPLOAD_SCRIPT_PATH),
            str(DUCKDB_FILE_PATH)
        ]
        if args.upload_dry_run:
            upload_command.append("--dry-run")
        if args.archive_item_identifier:
            upload_command.extend(["--identifier", args.archive_item_identifier])
        if args.archive_item_title:
            upload_command.extend(["--title", args.archive_item_title])
        if args.archive_item_description:
            upload_command.extend(["--description", args.archive_item_description])

        if not run_command(upload_command):
            print("Archive.org upload script failed.")
            # Depending on policy, we might not want to exit(1) for upload failure
            # For now, let's indicate failure but complete.
        else:
            print("--- Archive.org upload script completed. ---")
    else:
        print("Skipping Archive.org upload step as per --skip-upload.")

    print("\nPipeline execution finished.")

if __name__ == "__main__":
    # Ensure environment variables like IA_ACCESS_KEY, IA_SECRET_KEY are loaded
    # if not already set in the execution environment (e.g., by GitHub Actions).
    # from dotenv import load_dotenv
    # load_dotenv(dotenv_path=PROJECT_ROOT / '.env') # Assuming .env is in project root

    main()
