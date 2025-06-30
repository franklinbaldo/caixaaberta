# TODO - Caixa Aberta Migration & Enhancements

This document outlines potential next steps and areas for improvement following the initial migration to a dbt, DuckDB, and Archive.org based pipeline.

## Data Transformation & Modeling (dbt)

1.  **Expand dbt Seed Tests:**
    *   [ ] Add `schema.yml` configurations and tests (not_null, unique, accepted_values, etc.) for all 27 `imoveis_XX.csv` seed files. Currently, only a subset (AC, AL, SP) has detailed tests.
    *   [ ] Implement more sophisticated generic data tests (e.g., using `dbt-utils` or custom macros) for common columns across all seeds (e.g., `preco > 0`, `desconto` range).

2.  **Port Legacy Data Transformations to dbt Models:**
    *   [ ] **Geocoding:**
        *   Investigate methods to perform geocoding within dbt (e.g., Python models with `dbt-fal` or `dbt-py`, DuckDB UDFs calling external services if network access is feasible, or a post-dbt Python script).
        *   Re-implement the geocoding logic from the old `src/pipeline.py` and `geocoding_utils.py`.
    *   [ ] **Historical Tracking:**
        *   Design and implement dbt models (likely using snapshots or incremental models) to track changes over time, replicating `first_time_seen` and `not_seen_since` logic from the old `src/pipeline.py`.
    *   [ ] **Data Cleaning & Standardization:**
        *   Review `src/processador_caixa.py` and other cleaning logic in the old `src/pipeline.py`.
        *   Implement these cleaning steps as dbt models that transform the raw seed data.
    *   [ ] **Consolidated Table (`imoveis_BR`):**
        *   Create a dbt model to union all individual state tables (from seeds) into a single consolidated view or table, analogous to the old `imoveis_BR.csv`.

## Data Ingestion & Pipeline Orchestration

3.  **Automate Source CSV Download (Optional):**
    *   [ ] If the CSV files in `data/` are meant to be regularly updated from an external source, re-implement or create a script to automate their download (potentially adapting logic from the old `src/pipeline.py`).
    *   [ ] Integrate this download step into `src/run_dbt_pipeline.py` or the GitHub Actions workflow to run before `dbt build`.

4.  **Enhance Pipeline Scripts (`run_dbt_pipeline.py`, `upload_to_archive.py`):**
    *   [ ] Implement more robust error handling (e.g., try-except blocks for specific operations).
    *   [ ] Add structured logging using Python's `logging` module instead of just `print` statements.
    *   [ ] Allow more fine-grained control over Archive.org uploads (e.g., specifying an existing item identifier to update vs. always creating new).

## Archive.org Integration

5.  **Refine Archive.org Item Management:**
    *   [ ] Develop a clear strategy for versioning data on Archive.org (e.g., update existing items, create new items with versioned identifiers, use collections for versioning).
    *   [ ] Enhance `upload_to_archive.py` to support the chosen versioning strategy.
    *   [ ] Consider allowing more metadata fields to be passed dynamically for uploads.

## Testing

6.  **Expand Pytest Coverage:**
    *   [ ] Add tests for `src/run_dbt_pipeline.py` to verify argument parsing, command execution calls (mocking `subprocess`), and overall pipeline flow.
    *   [ ] Increase test coverage for edge cases in `src/upload_to_archive.py`.

## CI/CD (GitHub Actions)

7.  **GitHub Actions Workflow Enhancements:**
    *   [ ] Parameterize the `workflow_dispatch` trigger (e.g., allow specifying `--upload-dry-run` or target environment).
    *   [ ] Consider splitting build, test, and deploy/upload steps into separate jobs for better parallelism or conditional execution.
    *   [ ] Implement caching for Python dependencies (`uv` cache) and dbt packages/dependencies to speed up workflow runs.
    *   [ ] Add a step to validate `dbt_project.yml` and `profiles.yml` syntax.

## Documentation & Code Quality

8.  **Improve Documentation:**
    *   [ ] Add detailed descriptions for all dbt models and seeds within their respective `schema.yml` files.
    *   [ ] Generate and publish dbt documentation (`dbt docs generate` & `dbt docs serve`).
    *   [ ] Document the schema of the final DuckDB tables.

9.  **Code Refactoring & Cleanup:**
    *   [ ] Review and refactor Python scripts for clarity, efficiency, and adherence to coding standards (e.g., using a linter like Ruff, formatter like Black).
    *   [ ] Once all relevant functionalities are migrated, formally deprecate and remove unused legacy code from `src/pipeline.py`, `src/processador_caixa.py`, etc.
    *   [ ] Ensure `.env.sample` only contains variables relevant to the current architecture.

## Configuration & Security

10. **Review Configuration and Secrets Management:**
    *   [ ] Double-check that no sensitive information (apart from data intended for public archival) is accidentally committed or logged.
    *   [ ] Ensure `.gitignore` is comprehensive.
```
