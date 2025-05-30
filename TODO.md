# Project TODO List

## Critical Issues

- **Resolve `numpy`/`pandas` binary incompatibility:** Tests are failing due to a `ValueError: numpy.dtype size changed, may indicate binary incompatibility` when pandas is imported. This needs investigation in the development/CI environment.
- **Fix `psa.py` directory creation bug:** The line `file = Path(output, makedirs=True)` in `psa.py` is incorrect for ensuring the output directory for `imoveis_BR.csv` exists. It should be something like `Path(output).parent.mkdir(parents=True, exist_ok=True)`.
- **Handle initial `imoveis_BR.csv` creation in `psa.py`:** The script currently assumes `imoveis_BR.csv` exists when reading history. Add a check or try-except block to handle the case where the file doesn't exist on the first run.
- **Expand test coverage:** `test.py` only checks for an import. Add actual unit and integration tests for `etl.py` and `psa.py` functionalities.

## Important Enhancements

- **Update dependencies:** Review and update main dependencies (e.g., `pandas`, `requests`) and development dependencies (e.g., `pytest`, `black`, `isort`) in `pyproject.toml`. Regenerate `requirements.txt` and `requirements-dev.txt` with `uv pip compile` afterwards.
- **Improve error handling and logging:** Refactor `etl.py` and `psa.py` to use Python's `logging` module instead of `print()` for errors and operational messages. Implement more specific exception handling where appropriate.
- **Implement log rotation for `etl.log`:** The `etl.log` file currently grows indefinitely. Implement a log rotation mechanism (e.g., using `logging.handlers.RotatingFileHandler`).
- **Make URLs configurable in `etl.py`:** The hardcoded URLs for the data source could change. Move them to a configuration file or environment variables.
- **Optimize HTML parsing in `etl.py`:** The `extract_state` function parses the HTML content twice. Investigate if this can be optimized to a single parse.

## Future Considerations

- **Schema validation for CSV data:** Consider adding schema validation for the CSV files being read and written.
- **Refactor `etl.py` and `psa.py`:** Break down larger functions into smaller, more manageable units with clear responsibilities.
```
