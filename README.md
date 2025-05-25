[![Run every day](https://github.com/franklinbaldo/caixaaberta/actions/workflows/actions.yaml/badge.svg)](https://github.com/franklinbaldo/caixaaberta/actions/workflows/actions.yaml)

# What is Caixa Aberta?
Caixa Aberta is a script to scrap em compile realstate data from the Caixa Econômica Federal (CEF) official site.

# Why?
The CEF site is a great source of information, but it is not always easy to find the information you need.
Also, there is no history of the data, so it is hard follow what happened in the past.

# For who?
This script is for people who want to know what is happening in their city, state and country.
Scholars, students, real estate agents, financiers, etc.

# Project Structure and Workflow

The project consists of several key scripts that work together to scrape, process, and report on real estate data from Caixa Econômica Federal.

## 1. Data Scraping (primarily `etl.py`)
- The `etl.py` script (and associated utilities) is responsible for scraping real estate data for each Brazilian state from the Caixa website.
- It saves the data for each state into individual CSV files in the `data/` directory (e.g., `data/imoveis_AC.csv`, `data/imoveis_SP.csv`).

## 2. Data Processing and Historical Record Keeping (`psa.py`)
- The `psa.py` script (Persistent Staging Area) takes all the individual state CSV files from the `data/` directory and consolidates them into a single historical dataset: `imoveis_BR.csv`.
- **Data Cleaning**: During this process, financial data within the state CSVs (such as property prices, evaluation values, and discount percentages) is cleaned and standardized. For example, currency strings (like "R$ 1.234,56") and percentage strings are converted into numerical float types in `imoveis_BR.csv`. This cleaning logic is imported from `processador_caixa.py`.
- **Geocoding**: `psa.py` now also geocodes property addresses (from fields like `endereco`, `bairro`, `cidade`, `estado`) to obtain latitude and longitude coordinates. This process uses the `geopy` library with Nominatim (which relies on OpenStreetMap data). The geocoding logic is encapsulated in `geocoding_utils.py`.
- **Output File (`imoveis_BR.csv`)**: This file stores all unique property records encountered over time. Key columns include:
    - Original data fields from the scraped listings.
    - Cleaned financial data (as float types).
    - `latitude` (float): The geocoded latitude of the property. May be blank/NaN if geocoding was unsuccessful.
    - `longitude` (float): The geocoded longitude of the property. May be blank/NaN if geocoding was unsuccessful.
    - `first_time_seen` (date): The date when the property listing was first recorded by the script.
    - `not_seen_since` (date): The date when a previously seen property listing was no longer found in the scrape. This is `NaT` (Not a Time) for currently active listings.
- This script is typically run after `etl.py` has finished scraping the latest data.

## 3. Geocoding Utility (`geocoding_utils.py`)
- This utility script provides the function `get_coordinates_for_address(address_str)` which takes an address string and returns its latitude and longitude.
- It uses the `geopy` library with the Nominatim geocoder and includes rate limiting to respect service usage policies.
- It's used by `psa.py` to enrich property data with geographic coordinates.

## 4. Data Reporting (`reporter.py`)
- The `reporter.py` script provides a way to generate summary statistics from the consolidated `imoveis_BR.csv` file.
- **Purpose**: To quickly get an overview of the current dataset, including data quality metrics.
- **Usage**:
  ```bash
  python reporter.py
  ```
- **Output**: The script prints the following to the console:
    - Total number of properties listed.
    - Number of properties per state.
    - Average price of properties per state (formatted as R$).
    - Geocoding success statistics (overall and per state percentages, and raw counts).

## 5. Caixa CSV Processing Utility (`processador_caixa.py`)
- The `processador_caixa.py` script serves a dual role:
    - **Utility Module**: It provides data cleaning functions (specifically `limpar_colunas_financeiras`) that are imported and used by `psa.py` to standardize financial data before it's saved into `imoveis_BR.csv`.
    - **Standalone Script**: It can also be run directly (e.g., `python processador_caixa.py`) to process a single Caixa-originated CSV file (like the example `exemplo_imoveis.csv` it can generate, or one downloaded directly from Caixa's older systems if the format matches). When run directly, it reads a specified CSV, cleans it, and prints the processed DataFrame. This is useful for testing or inspecting individual files.

# Setup
To set up the project environment, you can use UV. If you don't have UV installed, you can install it with pip:
```bash
pip install uv
```
Alternatively, refer to the [official UV installation guide](https://github.com/astral-sh/uv#installation).

Once UV is installed, follow these steps:

```bash
# Create a virtual environment (optional but recommended)
# Using Python's built-in venv module:
python -m venv .venv
source .venv/bin/activate  # On Windows use .venv\Scripts\activate

# Or using UV:
uv venv
source .venv/bin/activate # On Windows use .venv\Scripts\activate

# Install dependencies
uv pip install -r requirements.txt

# Install development dependencies (optional)
uv pip install -r requirements-dev.txt
```

# TODO
- [ ] Better way to show the data
- [ ] Website
