#%%
# What is a Persistent Staging Area?
# Source system data is loaded into PSA without transformation
# Records are never deleted from PSA (archives may occur)
# PSA stores all unique records loaded (tracks history)
# Many more fields are stored in PSA than required by the data warehouse

from pathlib import Path

from etl import cols as etl_cols
from etl import log
from processador_caixa import limpar_colunas_financeiras # Import the cleaning function
import pandas as pd

cols = etl_cols + ["first_time_seen", "not_seen_since"]

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
]
sorting_cols.extend([col for col in cols if col not in sorting_cols])

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
            # Ensure all etl_cols are present, add if missing (important before cleaning)
            for col_name in etl_cols:
                if col_name not in df_state.columns:
                    df_state[col_name] = pd.NA # Or appropriate default like '' for strings
            
            df_state = limpar_colunas_financeiras(df_state,
                                                  coluna_preco='preco',
                                                  coluna_avaliacao='avaliacao',
                                                  coluna_desconto='desconto')
            cleaned_dfs.append(df_state.loc[:, etl_cols]) # Select etl_cols after cleaning
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
    # Group by etl_cols and aggregate 'first_time_seen' to get the minimum (earliest)
    # This assumes 'first_time_seen' might already exist in history with valid values.
    # For truly new items (only in current_data), first_time_seen will be NaT here.
    new_history['first_time_seen'] = new_history.groupby(etl_cols)['first_time_seen'].transform('min')

    new_history = new_history.drop_duplicates(subset=etl_cols, keep="last")

    now_timestamp = pd.Timestamp.now().normalize() # Use normalize to get date part if comparing dates

    # Initialize 'first_time_seen' for new records (those that were NaT after the groupby/transform)
    new_history['first_time_seen'] = new_history['first_time_seen'].fillna(now_timestamp)
    
    # Logic for 'not_seen_since'
    # Records from 'history' dataset were not in 'current_data', so update 'not_seen_since'
    new_history.loc[new_history['dataset'] == 'history', 'not_seen_since'] = now_timestamp
    
    # Records from 'current' dataset are active, so 'not_seen_since' should be NaT
    # If a record from history is also in current, 'dataset' becomes 'current' due to keep='last'
    new_history.loc[new_history['dataset'] == 'current', 'not_seen_since'] = pd.NaT
    
    # Ensure all sorting_cols are present in new_history before selecting them
    # This also includes 'first_time_seen' and 'not_seen_since' as they are part of 'cols'
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
