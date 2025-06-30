# Project TODO List

This document outlines the key tasks for improving the Real Estate Data Pipeline. The focus is on integrating disparate components, modernizing the workflow, and ensuring the pipeline is robust and maintainable.

## High Priority

### 1. Integrate Automated Data Fetching into the Workflow

-   **Context:** The current dbt pipeline relies on CSV files being manually placed in the `dbt_real_estate/seeds/` directory. This makes the data static and quickly outdated. The logic for downloading fresh data exists in the legacy `src/pipeline.py` but is not used by the main CI workflow.
-   **How to Fix:**
    1.  Create a new, focused script, for example, `src/fetch_data.py`.
    2.  Move the data downloading and parsing logic (functions `baixar_todos_csvs`, `_extract_data_for_state`, etc.) from `src/pipeline.py` into this new script.
    3.  Configure this script to save the downloaded CSVs directly into the `dbt_real_estate/seeds/` directory, overwriting any existing files to ensure freshness.
    4.  Add a step at the beginning of the GitHub Actions workflow (`.github/workflows/main.yml`) to execute this script before `dbt build` is run.

    ```yaml
    # In .github/workflows/main.yml, before the 'Run dbt tests' step
    - name: Fetch Latest Real Estate Data
      env:
        # The URL_BASE is needed for the fetcher script
        URL_BASE: "https://venda-imoveis.caixa.gov.br/listaweb/Lista_imoveis_{}.htm"
      run: |
        echo "Fetching latest data..."
        .venv/bin/python src/fetch_data.py
    ```

### 2. Re-introduce Geocoding as a dbt Transformation Step

-   **Context:** The valuable geocoding functionality, present in `src/geocoding_utils.py`, is currently dormant. The dbt pipeline only seeds raw data, resulting in a final database that lacks latitude and longitude coordinates, limiting its analytical use.
-   **How to Fix:** The most robust approach is to integrate geocoding into the dbt DAG (Directed Acyclic Graph) using a Python model.
    1.  **Create a staging model:** First, create a simple SQL model (e.g., `dbt_real_estate/models/staging/stg_imoveis.sql`) to `UNION ALL` data from all the individual seed tables into one comprehensive view.
    2.  **Create a Python dbt model:** Create a file like `dbt_real_estate/models/marts/imoveis_geocoded.py`. This model will `ref` the staging model.
    3.  **Implement the geocoding logic:** Inside the Python model, use Pandas to construct a full address string and apply the `get_coordinates_for_address` function from `src/geocoding_utils.py` to rows that are missing coordinates.
    4.  **Manage API Key:** Ensure the `GEOCODER_KEY` secret is passed from GitHub Actions to the `dbt build` command so the geocoding utility can authenticate.

    ```python
    # Example snippet for dbt_real_estate/models/marts/imoveis_geocoded.py
    import pandas as pd
    # This might require adjusting sys.path or pythonpath settings for dbt
    from geocoding_utils import get_coordinates_for_address

    def model(dbt, session):
        dbt.config(materialized="table")
        stg_imoveis_df = dbt.ref("stg_imoveis").df()

        # Construct full address for geocoding
        address_cols = ['endereco', 'bairro', 'cidade', 'estado']
        stg_imoveis_df['full_address'] = stg_imoveis_df[address_cols].fillna('').agg(', '.join, axis=1)

        # Apply geocoding (example, needs refinement for efficiency)
        coords = stg_imoveis_df['full_address'].apply(get_coordinates_for_address)
        stg_imoveis_df[['latitude', 'longitude']] = pd.DataFrame(coords.tolist(), index=stg_imoveis_df.index)

        return stg_imoveis_df
    ```

### 3. Clean Up and Deprecate Legacy `pipeline.py`

-   **Context:** The `src/pipeline.py` script contains a mix of old and new logic, including a consolidation flow that is now superseded by dbt. Its presence is confusing and creates maintenance overhead.
-   **How to Fix:**
    1.  Ensure the data fetching logic has been successfully migrated to a new script (as per Task #1).
    2.  Ensure the geocoding logic is understood and ready to be implemented within a dbt model (as per Task #2). `pipeline.py` can serve as a reference.
    3.  Once all useful functionality has been extracted, delete `src/pipeline.py` and the obsolete `imoveis_BR.csv` it generates.
    4.  Update the `README.md` to remove any references to running `pipeline.py`.
    5.  Review `pyproject.toml` to remove any dependencies that were exclusively used by the old script (e.g., `lxml`, if the new fetcher can work without it).

## Medium Priority

### 4. Update the Reporter to Use the DuckDB Database

-   **Context:** The `src/reporter.py` script is a great idea for providing a summary of the pipeline's output. However, it's currently configured to read `imoveis_BR.csv`, a file artifact from the legacy pipeline. It should instead report on the `real_estate_data.db` produced by dbt.
-   **How to Fix:**
    1.  Modify `src/reporter.py` to connect to the DuckDB database.
    2.  Instead of `pd.read_csv()`, use the `duckdb` library to query the final, geocoded dbt model.
    3.  Add a final step to the `.github/workflows/main.yml` to run the reporter, so a summary of each pipeline run is printed directly in the CI/CD logs.

    ```python
    # In src/reporter.py
    import duckdb
    import pandas as pd

    def generate_report(db_path="dbt_real_estate/real_estate_data.db"):
        con = duckdb.connect(db_path, read_only=True)
        # Assumes a final model named 'imoveis_geocoded' exists
        df = con.execute("SELECT * FROM imoveis_geocoded").fetchdf()
        con.close()
        # ... rest of the reporting logic ...
    ```

### 5. Expand Test Coverage

-   **Context:** The project has a good test suite for `upload_to_archive.py`, but other critical components like data fetching and geocoding are untested, making them brittle.
-   **How to Fix:**
    1.  **Test the Geocoder:** Create `tests/test_geocoding_utils.py`. Use `pytest-mock` to patch `geopy.geocoders.Nominatim` so that tests don't make real network calls. Test cache hits, cache misses, and handling of invalid addresses.
    2.  **Test the Data Fetcher:** Once `src/fetch_data.py` is created, write `tests/test_fetch_data.py`. Use a library like `requests-mock` to simulate HTTP responses from the Caixa website, allowing you to test the HTML parsing and data extraction logic reliably.

## Low Priority / Housekeeping

### 6. Refine dbt Seed Schema and Configuration

-   **Context:** The `dbt_real_estate/seeds/schema.yml` file contains repetitive test configurations for each state's CSV file. This can become difficult to manage.
-   **How to Fix:**
    1.  Continue using the current explicit `schema.yml` structure, but ensure it's complete for all 27 states, at least with primary key tests (`unique`, `not_null` on the `link` column).
    2.  Double-check the data types defined in `dbt_project.yml` under `seeds:+column_types` to ensure they are optimal for all columns, preventing potential casting errors during the `dbt seed` process.
    3.  (Future) For a more advanced setup, explore using a dbt macro to dynamically generate the test configurations for all seed files, reducing boilerplate code.
