#%%
import datetime
import argparse # For command-line arguments
from pathlib import Path # For path manipulation from psa.py
import os # For os.getenv
from dotenv import load_dotenv # For .env file loading

from lxml import html
import pandas as pd
import numpy as np # For pd.NA from psa.py
import requests

# From etl.py (now pipeline.py)
output_csv_template = "data/imoveis_{}.csv" # Renamed to avoid conflict
# base_url_template will be loaded from .env

# From psa.py
from processador_caixa import limpar_colunas_financeiras
from geocoding_utils import get_coordinates_for_address
# `etl_cols_original` from psa.py was `cols` in etl.py.
# `file_path` from psa.py for the final output
consolidated_output_csv = "imoveis_BR.csv"
# log = "etl.log" # Removed as per new requirement to use print

cols = [
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
# This 'cols' is specific to the data scraping part (baixar_csvs)
# It corresponds to etl_cols_original in psa.py
scraping_cols = cols

base_detalhe_url = "https://venda-imoveis.caixa.gov.br/sistema/detalhe-imovel.asp?hdnOrigem=index&hdnimovel="

brazilian_states = [
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
log = "etl.log"


# --- Functions for baixar_csvs ---
def _log_download_success(state: str, df_count: int) -> None:
    # assert not df.empty, "Empty dataframe for logging success" # df_count replaces df check
    if df_count > 0:
        print(f"[DOWNLOAD] Sucesso para o estado {state}: {df_count} registros baixados.")
    else:
        print(f"[DOWNLOAD] Aviso para o estado {state}: Nenhum registro baixado.")

def _extract_data_for_state(state: str, base_url_fmt_str: str) -> pd.DataFrame:
    if not base_url_fmt_str:
        print("Erro: URL_BASE não configurada. Verifique seu arquivo .env e a variável URL_BASE.")
        return pd.DataFrame(columns=scraping_cols)
    url = base_url_fmt_str.format(state)
    print(f"Baixando dados para o estado: {state} de {url}")
    reqs = requests.get(url)
    # tree = html.parse(reqs.text) # tree is not used if links are extracted from pd.read_html

    # Using the first table found by read_html
    try:
        extracted_df = pd.read_html(reqs.text, header=0)[0]
    except ValueError: # Handle cases where no table is found or table is empty
        print(f"Nenhuma tabela encontrada ou tabela vazia para o estado {state}.")
        return pd.DataFrame(columns=scraping_cols)


    extracted_df.columns = scraping_cols # Assign standard column names

    # Extracting links - this part needs careful checking with actual HTML structure
    # Assuming links are in the first column or identifiable correctly
    # The original xpath might be more robust if pd.read_html doesn't capture links well
    # For simplicity, if links are part of the table, they should be handled by column selection
    # If they need separate extraction:
    tree = html.fromstring(reqs.content) # Use fromstring for content
    links = [
        str(link.get("href")).replace(base_detalhe_url, "").strip()
        for link in tree.xpath("//table[@class='responsive sticky-enabled']/tbody/tr/td[1]/a") # Example XPath, needs verification
        if link.get("href") and "detalhe-imovel.asp" in link.get("href")
    ]
    if len(links) == len(extracted_df) and 'link' in scraping_cols:
        extracted_df["link"] = links
    elif 'link' in scraping_cols:
        # Fallback or warning if links cannot be matched
        print(f"Aviso: Não foi possível extrair links corretamente para {state} via XPath. Verifique a coluna 'link'.")
        # Ensure 'link' column exists, even if empty or filled by read_html incorrectly
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
    print("Transformando dados do estado...")
    if df.empty:
        return df

    # Bairro to uppercase
    if 'bairro' in df.columns:
        df["bairro"] = df["bairro"].fillna("").astype(str).str.upper().str.strip()

    # Sort and drop duplicates
    # Ensure all necessary columns for subset exist
    subset_cols = [col for col in ["estado", "cidade", "link"] if col in df.columns]
    if not subset_cols: # Cannot drop duplicates if key columns are missing
        print("Aviso: Colunas chave para deduplicação não encontradas. Pulando deduplicação.")
        return df

    df = df.sort_values(by=subset_cols)
    df = df.drop_duplicates(subset=subset_cols, keep="first")
    return df

def _load_state_data(df: pd.DataFrame, state: str) -> None:
    if df.empty:
        print(f"Nenhum dado para carregar para o estado {state}.")
        return
    output_file = output_csv_template.format(state)
    print(f"Carregando dados para {output_file}")
    df.to_csv(output_file, index=False)

def baixar_csvs_por_estado(state: str, base_url_fmt_str: str) -> None:
    """Baixa, transforma e salva dados de imóveis para um único estado."""
    print(f"Processando estado: {state}")
    extracted_df = _extract_data_for_state(state, base_url_fmt_str)
    if not extracted_df.empty:
        transformed_df = _transform_state_data(extracted_df)
        _load_state_data(transformed_df, state)
        _log_download_success(state, len(transformed_df))
    else:
        _log_download_success(state, 0) # Log that no data was extracted
        print(f"Nenhum dado extraído para o estado {state}.")

def baixar_todos_csvs(states_list: list, base_url_fmt_str: str) -> None:
    """Baixa dados de imóveis para uma lista de estados."""
    print("Iniciando download de CSVs para todos os estados...")
    Path("data").mkdir(parents=True, exist_ok=True) # Ensure data directory exists
    if not base_url_fmt_str:
        print("Erro: URL_BASE não fornecida para baixar_todos_csvs. Downloads serão pulados.")
        return
    for state in states_list:
        try:
            baixar_csvs_por_estado(state, base_url_fmt_str)
        except Exception as e:
            print(f"Erro ao processar o estado {state}: {e}")
# --- Fim das funções para baixar_csvs ---


# --- Funções para consolidar (adaptadas de psa.py) ---
# `cols` from psa.py (schema for imoveis_BR.csv)
# etl_cols_original from psa.py is scraping_cols here
# cols_psa = scraping_cols + ["latitude", "longitude", "first_time_seen", "not_seen_since"]
# sorting_cols_psa = [ # As defined in psa.py
#     "estado", "cidade", "bairro", "endereco", "preco", "avaliacao",
#     "desconto", "modalidade", "descricao", "link", "foto",
#     "latitude", "longitude",
# ]
# # Extend sorting_cols_psa to include any missing cols from cols_psa
# existing_sorting_keys_psa = set(sorting_cols_psa)
# sorting_cols_psa.extend([col for col in cols_psa if col not in existing_sorting_keys_psa])


def consolidar_csvs(output_filename: str = consolidated_output_csv) -> pd.DataFrame:
    print(f"Iniciando consolidação dos CSVs para {output_filename}...")
    history_file = Path(output_filename)

    # Define schema for the consolidated file, including tracking columns
    # This was `cols` in psa.py
    consolidated_schema = scraping_cols + ["latitude", "longitude", "first_time_seen", "not_seen_since"]

    if history_file.exists() and history_file.stat().st_size > 0:
        try:
            history_df = pd.read_csv(history_file)
        except Exception as e:
            print(f"Erro ao ler arquivo de histórico {history_file}: {e}. Iniciando com histórico vazio.")
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
            # Ensure all scraping_cols are present
            for col_name in scraping_cols:
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
            # This should align with scraping_cols + initialized lat/lon
            current_data_cols = scraping_cols + ['latitude', 'longitude']
            cleaned_dfs.append(df_state_cleaned.reindex(columns=current_data_cols, fill_value=pd.NA))

        except Exception as e:
            print(f"Erro ao processar o arquivo {csv_file}: {e}")
            continue

    if not cleaned_dfs:
        print("Nenhum CSV de estado encontrado ou processado. Consolidando apenas histórico (se existir).")
        # Ensure current_data is a DataFrame with the correct columns even if empty
        current_data_cols_for_empty = scraping_cols + ['latitude', 'longitude']
        current_data = pd.DataFrame(columns=current_data_cols_for_empty).assign(dataset="current")

    else:
        current_data = (
            pd.concat(cleaned_dfs, ignore_index=True)
            # Deduplicate current data based on core identity fields (scraping_cols)
            # Before merging with history
            .drop_duplicates(subset=scraping_cols, keep='first')
            .assign(dataset="current")
        )

    # Ensure current_data has all columns from consolidated_schema, filling missing ones with NA
    # This is important if scraping_cols does not perfectly match the start of consolidated_schema
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
    # Group by core identity fields (scraping_cols)
    if not combined_df.empty:
        combined_df['first_time_seen'] = combined_df.groupby(scraping_cols)['first_time_seen'].transform('min')

        # Deduplicate based on core identity fields, keeping the 'current' version if overlaps
        # This ensures that if a record was in history and is also in current, we take current's data
        # (which might have NA for lat/lon to be geocoded, or updated other fields)
        # and the 'first_time_seen' will be the earliest one.
        combined_df = combined_df.drop_duplicates(subset=scraping_cols, keep="last")

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
    print(f"Consolidação concluída. Arquivo salvo em: {output_filename}")
    return final_df_to_save
# --- Fim das funções para consolidar ---


# --- Função para geocodificar (adaptada de psa.py) ---
def geocodificar_dataframe(df: pd.DataFrame, api_key: str = None) -> pd.DataFrame:
    """
    Geocodifica endereços em um DataFrame que não possuem coordenadas.
    Requires GEOCODER_KEY to be set as an environment variable or passed as api_key.
    """
    print("Iniciando geocodificação...")
    # In psa.py, geocoding happened on rows where new_history['latitude'].isna()
    # We apply the same logic here.
    if 'latitude' not in df.columns:
        df['latitude'] = pd.NA
    if 'longitude' not in df.columns:
        df['longitude'] = pd.NA

    rows_to_geocode_idx = df[df['latitude'].isna()].index
    print(f"Identificados {len(rows_to_geocode_idx)} registros para geocodificação.")

    if not rows_to_geocode_idx.empty:
        # GEOCODER_KEY should be loaded from .env or os.getenv elsewhere if not passed
        # For now, direct use of get_coordinates_for_address assumes it handles key access
        for index in rows_to_geocode_idx:
            row = df.loc[index]
            address_parts = [
                row.get('endereco', ''),
                row.get('bairro', ''),
                row.get('cidade', ''),
                row.get('estado', '')
            ]
            # Filter out None, NaN, or empty strings before joining
            address_str = ", ".join(str(part) for part in address_parts if pd.notna(part) and str(part).strip())

            if address_str:
                print(f"Geocodificando: {address_str}")
                # Pass api_key to the geocoding function if provided
                lat, lon = get_coordinates_for_address(address_str, api_key=api_key)
                df.loc[index, 'latitude'] = lat
                df.loc[index, 'longitude'] = lon
            else:
                print(f"Skipping geocodificação para linha {index} devido a endereço vazio.")
    else:
        print("Nenhum registro necessita de geocodificação.")

    print("Geocodificação concluída.")
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

    # Load environment variables
    url_base = os.getenv("URL_BASE")
    geocoder_api_key = os.getenv("GEOCODER_KEY")

    if not args.skip_download:
        if not url_base:
            print("Aviso: URL_BASE não definida no .env. Etapa de download será pulada.")
        else:
            baixar_todos_csvs(brazilian_states, url_base)
    else:
        print("Pulando etapa de download de CSVs dos estados.")

    # Consolidar sempre ocorre
    consolidated_df = consolidar_csvs(output_filename=consolidated_output_csv)

    if args.geo:
        if not geocoder_api_key:
            print("Aviso: GEOCODER_KEY não definida no .env. A geocodificação pode não funcionar como esperado ou usar limites padrão.")

        # Pass the API key (even if None) to the geocoding function
        geocoded_df = geocodificar_dataframe(consolidated_df.copy(), api_key=geocoder_api_key) # Pass copy

        # Re-salvar o arquivo consolidado com os dados geocodificados
        # Need to ensure sorting and date formatting is consistent with consolidar_csvs output
        final_sorting_cols = [
            "estado", "cidade", "bairro", "endereco", "preco", "avaliacao",
            "desconto", "modalidade", "descricao", "link", "foto",
            "latitude", "longitude", "first_time_seen", "not_seen_since"
        ]
        consolidated_schema_for_geocoded_output = scraping_cols + ["latitude", "longitude", "first_time_seen", "not_seen_since"]

        final_output_columns_geocoded = [col for col in final_sorting_cols if col in geocoded_df.columns]
        missing_cols_geocoded = [col for col in consolidated_schema_for_geocoded_output if col not in final_output_columns_geocoded]
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

        geocoded_df_to_save.to_csv(consolidated_output_csv, index=False)
        print(f"Arquivo consolidado e geocodificado salvo em: {consolidated_output_csv}")
    else:
        print("Geocodificação não ativada. O arquivo consolidado não foi modificado após a consolidação inicial.")

if __name__ == "__main__":
    # TODO: Add .env loading here if GEOCODER_KEY is needed for geocoding_utils
    load_dotenv() # Carrega variáveis de ambiente do arquivo .env
    main()
