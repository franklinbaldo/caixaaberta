#%%
# What is a Persistent Staging Area?
# Source system data is loaded into PSA without transformation
# Records are never deleted from PSA (archives may occur)
# PSA stores all unique records loaded (tracks history)
# Many more fields are stored in PSA than required by the data warehouse

from pathlib import Path

from etl import cols as etl_cols_original # Keep original etl_cols for core identity
from etl import log
from processador_caixa import limpar_colunas_financeiras
from geocoding_utils import get_coordinates_for_address # Import for geocoding
import pandas as pd
import numpy as np # For pd.NA

# Define etl_cols for psa.py's internal use if it differs from etl.py's original
# For this task, psa.py will add lat/lon, but etl_cols for deduplication remains the original.
etl_cols = etl_cols_original 

# 'cols' defines the full schema of imoveis_BR.csv, including history
cols = etl_cols + ["latitude", "longitude", "first_time_seen", "not_seen_since"]

sorting_cols = [
    "estado",
    "cidade",
    "bairro",
    "endereco",
    "preco",
    "avaliacao",
    "desconto",
    "modalidade",
    "descricao",
    "link",
    "foto",
    "latitude", # Add to sorting
    "longitude", # Add to sorting
]
# Ensure all columns from 'cols' (which now includes lat/lon and timestamps) are in sorting_cols
# if they are not part of the primary sorting keys already listed.
# This maintains all columns in the final output.
existing_sorting_keys = set(sorting_cols)
sorting_cols.extend([col for col in cols if col not in existing_sorting_keys])


file_path = "imoveis_BR.csv"


def get_last_etl() -> dict:
    df = (
        pd.read_csv(
            log,
            header=None,
            names=["state", "date"],
            parse_dates=True,
            infer_datetime_format=True,
            dtype="str",
        )
        .assign(date=lambda df: pd.to_datetime(df["date"]))
        .drop_duplicates(keep="last")
        .set_index("state")
    )
    return df.date.to_dict()


def update_records(output="imoveis_BR.csv") -> pd.DataFrame:
    """
    Updates records in PSA
    """
    #%%
    file = Path(output) 
    if file.exists() and file.stat().st_size > 0:
        # Ensure all expected columns from 'cols' are present when reading history, fill with NA if not
        try:
            history_df = pd.read_csv(file)
        except Exception as e:
            print(f"Error reading history file {file}: {e}. Initializing empty history.")
            history_df = pd.DataFrame() # Empty dataframe if read fails

        # Add missing columns from 'cols' to history_df
        for col_name in cols:
            if col_name not in history_df.columns:
                history_df[col_name] = pd.NA
        history = history_df[cols].assign(dataset="history")
    else:
        history = pd.DataFrame(columns=cols).assign(dataset="history")

    states_files = Path("data/").glob("imoveis_*.csv")
    cleaned_dfs = []
    for csv_file in states_files:
        try:
            df_state = pd.read_csv(csv_file)
            # Ensure all original etl_cols are present, add if missing
            for col_name in etl_cols_original: # Use original for reading state files
                if col_name not in df_state.columns:
                    df_state[col_name] = pd.NA
            
            df_state_cleaned_finance = limpar_colunas_financeiras(df_state,
                                                                  coluna_preco='preco',
                                                                  coluna_avaliacao='avaliacao',
                                                                  coluna_desconto='desconto')
            
            # Initialize latitude and longitude columns before appending
            df_state_cleaned_finance['latitude'] = pd.NA
            df_state_cleaned_finance['longitude'] = pd.NA
            
            # Select all columns that should come from state files: original etl_cols + new lat/lon placeholders
            cols_from_state_files = etl_cols_original + ['latitude', 'longitude']
            cleaned_dfs.append(df_state_cleaned_finance.loc[:, cols_from_state_files])

        except Exception as e:
            print(f"Error processing file {csv_file}: {e}")
            continue # Skip problematic files

    if not cleaned_dfs:
        current_data = pd.DataFrame(columns=etl_cols).assign(dataset="current")
    else:
        current_data = (
            pd.concat(cleaned_dfs, ignore_index=True)
            .drop_duplicates(subset=etl_cols) # Deduplicate current data before merging with history
            .assign(dataset="current")
        )

    new_history = pd.concat([history, current_data], ignore_index=True, sort=False)
    
    # Before dropping duplicates, ensure 'first_time_seen' is carried over from history
    # Group by original etl_cols (core identity fields) and aggregate 'first_time_seen'
    new_history['first_time_seen'] = new_history.groupby(etl_cols_original)['first_time_seen'].transform('min')
    
    # Deduplicate based on the core identity fields.
    # If history had coordinates, and current data matches on etl_cols_original,
    # keep="last" means the 'current' version (with NA for lat/lon at this stage) will be kept.
    # This record will then be picked up by the geocoding step if its latitude is NA.
    new_history = new_history.drop_duplicates(subset=etl_cols_original, keep="last")

    # Geocoding step for rows in new_history where latitude is still NA.
    # This will geocode records that are new, or records from history that never had coordinates,
    # or records whose historical coordinates were "overwritten" by NA from current_data during deduplication.
    rows_to_geocode_idx = new_history[new_history['latitude'].isna()].index
    print(f"Identified {len(rows_to_geocode_idx)} records for geocoding.")
    for index in rows_to_geocode_idx:
        row = new_history.loc[index]
        address_parts = [
            row.get('endereco', ''),
            row.get('bairro', ''),
            row.get('cidade', ''),
            row.get('estado', '')
        ]
        address_str = ", ".join(str(part) for part in address_parts if pd.notna(part) and str(part).strip())
        
        if address_str:
            print(f"Geocoding: {address_str}") # Optional: log address being geocoded
            lat, lon = get_coordinates_for_address(address_str)
            new_history.loc[index, 'latitude'] = lat
            new_history.loc[index, 'longitude'] = lon
        # else: print(f"Skipping geocoding for row {index} due to empty address string.") # Optional

    now_timestamp = pd.Timestamp.now().normalize()

    new_history['first_time_seen'] = new_history['first_time_seen'].fillna(now_timestamp)
    new_history.loc[new_history['dataset'] == 'history', 'not_seen_since'] = now_timestamp
    new_history.loc[new_history['dataset'] == 'current', 'not_seen_since'] = pd.NaT
    
    # Ensure all sorting_cols are present in new_history before selecting them
    # 'cols' (which sorting_cols is derived from) now includes latitude, longitude
    final_cols_to_keep = []
    for col_name in sorting_cols: # sorting_cols is defined globally
        if col_name not in new_history.columns:
            new_history[col_name] = pd.NA # Add if missing
        final_cols_to_keep.append(col_name)
    
    # Add timestamp columns if they are not already in final_cols_to_keep (they should be via 'cols')
    if 'first_time_seen' not in final_cols_to_keep: final_cols_to_keep.append('first_time_seen')
    if 'not_seen_since' not in final_cols_to_keep: final_cols_to_keep.append('not_seen_since')
        
    # Select and prepare final DataFrame
    final_df = new_history[final_cols_to_keep]

    # Convert date columns to date objects (without time)
    date_cols_to_convert = ['first_time_seen', 'not_seen_since']
    for date_col in date_cols_to_convert:
        if date_col in final_df.columns:
            final_df[date_col] = pd.to_datetime(final_df[date_col]).dt.date

    final_df = final_df.sort_values(by=sorting_cols).reset_index(drop=True)
    
    final_df.to_csv(file_path, index=False)
    print("Arquivo atualizado com sucesso!", file_path)
    #%%
    return new_history


# %%
if __name__ == "__main__":
    update_records()
