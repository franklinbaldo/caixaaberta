#%%
import datetime
import argparse
from pathlib import Path
import os
import logging # New logging import
import sys # For logging stream
from dotenv import load_dotenv

from lxml import html
import pandas as pd
import numpy as np
import requests

# Relative imports for package structure
from .processador_caixa import limpar_colunas_financeiras
from .geocoding_utils import get_coordinates_for_address
# Cache functions will be used by geocoding_utils directly or passed if needed.
# from .cache import init_cache_db, clear_cache # Example if pipeline managed cache lifecycle

# --- Constants ---
BASE_URL = "https://venda-imoveis.caixa.gov.br/listaweb/Lista_imoveis_{}.htm" # Hardcoded
OUTPUT_CSV_TEMPLATE = "data/imoveis_{}.csv"
CONSOLIDATED_OUTPUT_CSV = "imoveis_BR.csv"
# LOG_FILE = "etl.log" # Replaced by logging module

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format="%(asctime)s | %(levelname)s | %(message)s")
# --- End Logging Setup ---

# Define column names for scraping (remains same as before)
SCRAPING_COLS = [
    "link",
    "endereco",
    "bairro",
    "descricao",
    "preco",
    "avaliacao",
    "desconto",
    "modalidade",
    "foto",
    "cidade",
    "estado",
]
# It corresponds to etl_cols_original in psa.py
# scraping_cols = cols # Already assigned above

BASE_DETALHE_URL = "https://venda-imoveis.caixa.gov.br/sistema/detalhe-imovel.asp?hdnOrigem=index&hdnimovel="

BRAZILIAN_STATES = [
    "AC",
    "AM",
    "AL",
    "AP",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MT",
    "MS",
    "MG",
    "PA",
    "PB",
    "PR",
    "PE",
    "PI",
    "RJ",
    "RN",
    "RS",
    "RO",
    "RR",
    "SC",
    "SP",
    "SE",
    "TO",
]
# log = "etl.log" # Replaced by logging module


# --- Functions for baixar_csvs ---
def _log_download_success(state: str, df_count: int) -> None:
    if df_count > 0:
        logging.info(f"Download success for state {state}: {df_count} records downloaded.")
    else:
        logging.warning(f"Download warning for state {state}: No records downloaded.")

def _extract_data_for_state(state: str) -> pd.DataFrame:
    url = BASE_URL.format(state)
    logging.info(f"Downloading data for state: {state} from {url}")
    reqs = requests.get(url)

    try:
        extracted_df = pd.read_html(reqs.text, header=0)[0]
    except ValueError:
        logging.warning(f"No table found or table empty for state {state}.")
        return pd.DataFrame(columns=SCRAPING_COLS)

    extracted_df.columns = SCRAPING_COLS

    tree = html.fromstring(reqs.content)
    links = [
        str(link.get("href")).replace(BASE_DETALHE_URL, "").strip()
        for link in tree.xpath("//table[@class='responsive sticky-enabled']/tbody/tr/td[1]/a")
        if link.get("href") and "detalhe-imovel.asp" in link.get("href")
    ]
    if len(links) == len(extracted_df) and 'link' in SCRAPING_COLS:
        extracted_df["link"] = links
    elif 'link' in SCRAPING_COLS:
        logging.warning(f"Could not extract links correctly for {state} via XPath. Check 'link' column.")
        if "link" not in extracted_df.columns:
             extracted_df["link"] = pd.NA


    # Monetary conversion, keeping original logic for now
    for col_name in ["preco", "avaliacao"]:
        if col_name in extracted_df.columns:
            extracted_df[col_name] = (
                extracted_df[col_name]
                .astype(str)
                .str.replace("R$", "", regex=False)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
                .pipe(lambda x: pd.to_numeric(x, errors='coerce')) # Coerce errors to NaT/NaN
            )
    return extracted_df

def _transform_state_data(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Transforming state data...")
    if df.empty:
        return df

    # Bairro to uppercase
    if 'bairro' in df.columns:
        df["bairro"] = df["bairro"].fillna("").astype(str).str.upper().str.strip()

    # Sort and drop duplicates
    # Ensure all necessary columns for subset exist
    subset_cols = [col for col in ["estado", "cidade", "link"] if col in df.columns]
    if not subset_cols:
        logging.warning("Key columns for deduplication not found. Skipping deduplication.")
        return df

    df = df.sort_values(by=subset_cols)
    df = df.drop_duplicates(subset=subset_cols, keep="first")
    return df

def _load_state_data(df: pd.DataFrame, state: str) -> None:
    if df.empty:
        logging.info(f"No data to load for state {state}.")
        return
    output_file = OUTPUT_CSV_TEMPLATE.format(state)
    logging.info(f"Loading data to {output_file}")
    df.to_csv(output_file, index=False)

def baixar_csvs_por_estado(state: str) -> None:
    """Baixa, transforma e salva dados de imóveis para um único estado."""
    logging.info(f"Processing state: {state}")
    extracted_df = _extract_data_for_state(state)
    if not extracted_df.empty:
        transformed_df = _transform_state_data(extracted_df)
        _load_state_data(transformed_df, state)
        _log_download_success(state, len(transformed_df))
    else:
        _log_download_success(state, 0)
        logging.info(f"No data extracted for state {state}.")

def baixar_todos_csvs(states_list: list) -> None:
    """Baixa dados de imóveis para uma lista de estados."""
    logging.info("Starting download of CSVs for all states...")
    Path("data").mkdir(parents=True, exist_ok=True)
    for state in states_list:
        try:
            baixar_csvs_por_estado(state)
        except Exception as e:
            logging.error(f"Error processing state {state}: {e}", exc_info=True)
# --- Fim das funções para baixar_csvs ---


# --- Funções para consolidar (adaptadas de psa.py) ---
def consolidar_csvs(output_filename: str = CONSOLIDATED_OUTPUT_CSV) -> pd.DataFrame:
    logging.info(f"Starting consolidation of CSVs to {output_filename}...")
    history_file = Path(output_filename)

    # Define schema for the consolidated file, including tracking columns
    # This was `cols` in psa.py
    consolidated_schema = SCRAPING_COLS + ["latitude", "longitude", "first_time_seen", "not_seen_since"]

    if history_file.exists() and history_file.stat().st_size > 0:
        try:
            history_df = pd.read_csv(history_file)
        except Exception as e:
            logging.error(f"Error reading history file {history_file}: {e}. Initializing empty history.", exc_info=True)
            history_df = pd.DataFrame()

        # Ensure all columns from consolidated_schema exist in history_df
        for col_name in consolidated_schema:
            if col_name not in history_df.columns:
                history_df[col_name] = pd.NA # Use pd.NA for pandas native nullable types
        history = history_df[consolidated_schema].assign(dataset="history")
    else:
        history = pd.DataFrame(columns=consolidated_schema).assign(dataset="history")

    state_files = Path("data/").glob("imoveis_*.csv")
    cleaned_dfs = []
    for csv_file in state_files:
        try:
            df_state = pd.read_csv(csv_file)
            # Ensure all SCRAPING_COLS are present
            for col_name in SCRAPING_COLS:
                if col_name not in df_state.columns:
                    df_state[col_name] = pd.NA

            # Financial columns cleaning (using the function from processador_caixa)
            # Assuming 'preco', 'avaliacao', 'desconto' are the correct column names in individual CSVs
            df_state_cleaned = limpar_colunas_financeiras(df_state,
                                                          coluna_preco='preco',
                                                          coluna_avaliacao='avaliacao',
                                                          coluna_desconto='desconto')

            # Initialize lat/lon for current data (will be filled by geocoding if needed)
            df_state_cleaned['latitude'] = pd.NA
            df_state_cleaned['longitude'] = pd.NA

            # Select columns relevant for current data before merging with history
            # This should align with SCRAPING_COLS + initialized lat/lon
            current_data_cols = SCRAPING_COLS + ['latitude', 'longitude']
            cleaned_dfs.append(df_state_cleaned.reindex(columns=current_data_cols, fill_value=pd.NA))

        except Exception as e:
            logging.error(f"Error processing file {csv_file}: {e}", exc_info=True)
            continue

    if not cleaned_dfs:
        logging.info("No state CSVs found or processed. Consolidating history only (if it exists).")
        # Ensure current_data is a DataFrame with the correct columns even if empty
        current_data_cols_for_empty = SCRAPING_COLS + ['latitude', 'longitude']
        current_data = pd.DataFrame(columns=current_data_cols_for_empty).assign(dataset="current")

    else:
        current_data = (
            pd.concat(cleaned_dfs, ignore_index=True)
            # Deduplicate current data based on core identity fields (SCRAPING_COLS)
            # Before merging with history
            .drop_duplicates(subset=SCRAPING_COLS, keep='first')
            .assign(dataset="current")
        )

    # Ensure current_data has all columns from consolidated_schema, filling missing ones with NA
    # This is important if SCRAPING_COLS does not perfectly match the start of consolidated_schema
    # or if some columns were unexpectedly dropped.
    for col_name in consolidated_schema:
        if col_name not in current_data.columns:
            current_data[col_name] = pd.NA
    current_data = current_data.reindex(columns=consolidated_schema, fill_value=pd.NA)


    # Combine history and current data
    # Ensure both dataframes have 'dataset' column before concat
    if 'dataset' not in history.columns and not history.empty:
        history['dataset'] = 'history'
    if 'dataset' not in current_data.columns and not current_data.empty:
        current_data['dataset'] = 'current'

    combined_df = pd.concat([history, current_data], ignore_index=True, sort=False)

    # Handle timestamps: 'first_time_seen' and 'not_seen_since'
    # Carry over 'first_time_seen' from history for records that existed
    # Group by core identity fields (SCRAPING_COLS)
    if not combined_df.empty:
        combined_df['first_time_seen'] = combined_df.groupby(SCRAPING_COLS)['first_time_seen'].transform('min')

        # Deduplicate based on core identity fields, keeping the 'current' version if overlaps
        # This ensures that if a record was in history and is also in current, we take current's data
        # (which might have NA for lat/lon to be geocoded, or updated other fields)
        # and the 'first_time_seen' will be the earliest one.
        combined_df = combined_df.drop_duplicates(subset=SCRAPING_COLS, keep="last")

        now_timestamp = pd.Timestamp.now().normalize()
        combined_df['first_time_seen'] = combined_df['first_time_seen'].fillna(now_timestamp)

        # Update 'not_seen_since'
        # If a record was in 'history' (dataset='history') but is NOT in 'current' after deduplication,
        # it implies it's no longer seen. This logic is tricky with keep='last'.
        # A better way: find records in history whose scraping_cols are not in current_data's scraping_cols

        # Reset 'not_seen_since' for all records that are currently present
        combined_df.loc[combined_df['dataset'] == 'current', 'not_seen_since'] = pd.NaT

        # For records that were only in history (their 'dataset' column would still be 'history'
        # after the drop_duplicates if they weren't in current_data), set 'not_seen_since'.
        # This assumes 'dataset' correctly reflects the origin after drop_duplicates.
        # If a record from history is *also* in current, drop_duplicates(keep='last') keeps the one from current_data.
        # So, if dataset == 'history' after drop_duplicates, it means it was *only* in history.
        combined_df.loc[combined_df['dataset'] == 'history', 'not_seen_since'] = \
            combined_df.loc[combined_df['dataset'] == 'history', 'not_seen_since'].fillna(now_timestamp)


    # Define sorting columns for the final output, ensuring all consolidated_schema columns are present
    final_sorting_cols = [
        "estado", "cidade", "bairro", "endereco", "preco", "avaliacao",
        "desconto", "modalidade", "descricao", "link", "foto",
        "latitude", "longitude", "first_time_seen", "not_seen_since" # Ensure all are here
    ]
    # Ensure all columns from consolidated_schema are in final_sorting_cols
    # This also defines the column order for the output CSV
    final_output_columns = [col for col in final_sorting_cols if col in combined_df.columns]
    missing_cols = [col for col in consolidated_schema if col not in final_output_columns]
    for mc in missing_cols: # Add any schema columns that might have been missed in sorting list
        if mc not in final_output_columns:
             final_output_columns.append(mc)

    # Add any other columns that might be in combined_df but not in final_output_columns yet
    # (e.g. 'dataset' column, though it's usually dropped before final save)
    # For this task, we only want consolidated_schema in the final output.
    final_df_to_save = combined_df.reindex(columns=final_output_columns, fill_value=pd.NA)


    # Convert date columns to date objects (without time) before saving
    date_cols_to_convert = ['first_time_seen', 'not_seen_since']
    for date_col in date_cols_to_convert:
        if date_col in final_df_to_save.columns:
            final_df_to_save[date_col] = pd.to_datetime(final_df_to_save[date_col], errors='coerce').dt.date

    # Sort the DataFrame
    # Ensure sorting columns actually exist in the dataframe to avoid errors
    actual_sorting_cols = [col for col in final_sorting_cols if col in final_df_to_save.columns]
    if actual_sorting_cols:
        final_df_to_save = final_df_to_save.sort_values(by=actual_sorting_cols).reset_index(drop=True)

    # Save the consolidated DataFrame (without geocoding yet for this function)
    final_df_to_save.to_csv(output_filename, index=False)
    logging.info(f"Consolidation complete. File saved to: {output_filename}")
    return final_df_to_save
# --- Fim das funções para consolidar ---


# --- Função para geocodificar (adaptada de psa.py) ---
def geocodificar_dataframe(df: pd.DataFrame, api_key: str = None) -> pd.DataFrame:
    """
    Geocodifica endereços em um DataFrame que não possuem coordenadas.
    Requires GEOCODER_KEY to be set as an environment variable or passed as api_key.
    """
    logging.info("Starting geocoding...")
    # In psa.py, geocoding happened on rows where new_history['latitude'].isna()
    # We apply the same logic here.
    if 'latitude' not in df.columns:
        df['latitude'] = pd.NA
    if 'longitude' not in df.columns:
        df['longitude'] = pd.NA

    rows_to_geocode_idx = df[df['latitude'].isna()].index
    logging.info(f"Identified {len(rows_to_geocode_idx)} records for geocoding.")

    if not rows_to_geocode_idx.empty:
        for index in rows_to_geocode_idx:
            row = df.loc[index]
            address_parts = [
                row.get('endereco', ''),
                row.get('bairro', ''),
                row.get('cidade', ''),
                row.get('estado', '')
            ]
            address_str = ", ".join(str(part) for part in address_parts if pd.notna(part) and str(part).strip())

            if address_str:
                logging.debug(f"Geocoding address: {address_str}") # Changed to debug, can be verbose
                lat, lon = get_coordinates_for_address(address_str, api_key=api_key)
                df.loc[index, 'latitude'] = lat
                df.loc[index, 'longitude'] = lon
            else:
                logging.debug(f"Skipping geocoding for row index {index} due to empty address string.") # Changed to debug
    else:
        logging.info("No records require geocoding.")

    logging.info("Geocoding process finished.")
    return df
# --- Fim da função para geocodificar ---


# --- Main execution logic ---
def main():
    parser = argparse.ArgumentParser(description="Pipeline de ETL para imóveis da Caixa.")
    parser.add_argument(
        "--geo",
        action="store_true",
        help="Ativar geocodificação. Requer chave de API do geocodificador (ex: GEOCODER_KEY no .env)."
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Pular a etapa de download de CSVs dos estados."
    )
    args = parser.parse_args()

    # Load environment variables (only GEOCODER_KEY now)
    # url_base = os.getenv("URL_BASE") # Removed, BASE_URL is a constant
    geocoder_api_key = os.getenv("GEOCODER_KEY")

    if not args.skip_download:
        baixar_todos_csvs(BRAZILIAN_STATES)
    else:
        logging.info("Skipping download of state CSVs.")

    # Consolidar sempre ocorre
    consolidated_df = consolidar_csvs(output_filename=CONSOLIDATED_OUTPUT_CSV)

    if args.geo:
        if not geocoder_api_key:
            logging.warning("GEOCODER_KEY not defined in .env. Geocoding may not work as expected or use default limits.")

        geocoded_df = geocodificar_dataframe(consolidated_df.copy(), api_key=geocoder_api_key)

        # Re-salvar o arquivo consolidado com os dados geocodificados
        # Need to ensure sorting and date formatting is consistent with consolidar_csvs output
        final_sorting_cols = [ # These are effectively the columns of `consolidated_schema`
            "estado", "cidade", "bairro", "endereco", "preco", "avaliacao",
            "desconto", "modalidade", "descricao", "link", "foto",
            "latitude", "longitude", "first_time_seen", "not_seen_since"
        ]
        # consolidated_schema_for_geocoded_output was SCRAPING_COLS + tracking/geo cols
        # This is the same as `consolidated_schema` defined in `consolidar_csvs`
        # Re-using that definition for clarity or ensuring it's passed around might be better,
        # but for now, this re-definition is okay as structure is identical.
        current_consolidated_schema = SCRAPING_COLS + ["latitude", "longitude", "first_time_seen", "not_seen_since"]


        final_output_columns_geocoded = [col for col in final_sorting_cols if col in geocoded_df.columns]
        # Ensure all columns from the defined schema are present, even if not explicitly in sorting_cols list
        missing_cols_geocoded = [col for col in current_consolidated_schema if col not in final_output_columns_geocoded]
        for mc in missing_cols_geocoded:
             if mc not in final_output_columns_geocoded: final_output_columns_geocoded.append(mc)

        geocoded_df_to_save = geocoded_df.reindex(columns=final_output_columns_geocoded, fill_value=pd.NA)

        date_cols_to_convert = ['first_time_seen', 'not_seen_since']
        for date_col in date_cols_to_convert:
            if date_col in geocoded_df_to_save.columns:
                geocoded_df_to_save[date_col] = pd.to_datetime(geocoded_df_to_save[date_col], errors='coerce').dt.date

        actual_sorting_cols_geocoded = [col for col in final_sorting_cols if col in geocoded_df_to_save.columns]
        if actual_sorting_cols_geocoded:
            geocoded_df_to_save = geocoded_df_to_save.sort_values(by=actual_sorting_cols_geocoded).reset_index(drop=True)

        geocoded_df_to_save.to_csv(CONSOLIDATED_OUTPUT_CSV, index=False)
        logging.info(f"Consolidated and geocoded file saved to: {CONSOLIDATED_OUTPUT_CSV}")
    else:
        logging.info("Geocoding not activated. Consolidated file was not modified after initial consolidation.")

if __name__ == "__main__":
    load_dotenv() # Load environment variables from .env (primarily for GEOCODER_KEY)
    main()
