#%%
# What is a Persistent Staging Area?
# Source system data is loaded into PSA without transformation
# Records are never deleted from PSA (archives may occur)
# PSA stores all unique records loaded (tracks history)
# Many more fields are stored in PSA than required by the data warehouse

from pathlib import Path

from etl import cols as etl_cols
from etl import log
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
            # parse_dates=True, # Let pd.to_datetime handle parsing
            # infer_datetime_format=True, # Deprecated
            dtype={"state": str, "date": str}, # Read as string first
        )
        .assign(date=lambda df: pd.to_datetime(df["date"], errors='coerce')) # Coerce errors to NaT
        .drop_duplicates(subset=["state"], keep="last") # Ensure drop_duplicates is on state
        .set_index("state")
    )
    return df.date.to_dict()


def update_records(output="imoveis_BR.csv") -> pd.DataFrame:
    """
    Updates records in PSA
    """
    #%%
    output_path = Path(output)
    # Ensure parent directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        history = pd.read_csv(output_path)[cols].assign(dataset="history")  # Get the history
    else:
        history = pd.DataFrame(columns=cols).assign(dataset="history") # Empty history if file doesn't exist
        
    states_files = Path("data/").glob("imoveis_*.csv")  # Get the states files
    # Filter out empty or problematic CSVs early
    def read_csv_safe(csv_file):
        # Simplified for extreme debugging - directly try to read and return.
        # This removes the df.empty check and some error handling to isolate the issue.
        try:
            df = pd.read_csv(csv_file, sep=',')
            # Crucial debug: print shape AND if it's considered empty by pandas right after load
            print(f"DEBUG read_csv_safe (simplified): Read {csv_file}, shape {df.shape}, df.empty is {df.empty}, head: \n{df.head()}")
            # If df.empty is True despite shape (1,11), that's the core issue.
            # For now, if it's truly empty by shape, then return None.
            if df.shape[0] == 0: # A more direct check than df.empty for this debug
                 print(f"Warning: {csv_file} resulted in 0 rows, skipping.")
                 return None
            return df
        except Exception as e:
            print(f"ERROR in simplified read_csv_safe for {csv_file}: {e}")
            return None
            
    states_dfs_gen = (read_csv_safe(csv) for csv in states_files)
    # Explicitly convert generator to list and print for debugging
    states_dfs_list = list(states_dfs_gen)
    print(f"DEBUG psa.py: states_dfs_list (before filtering): {states_dfs_list}")
    
    states_dfs_filtered = [df for df in states_dfs_list if df is not None]
    print(f"DEBUG psa.py: states_dfs_filtered (after filtering Nones): {states_dfs_filtered}")
    print(f"DEBUG psa.py: Length of states_dfs_filtered: {len(states_dfs_filtered)}")

    if not states_dfs_filtered: # No valid state CSVs found
        print("DEBUG psa.py: No valid state CSVs found, creating empty current_data.")
        current_data = pd.DataFrame(columns=etl_cols).assign(dataset="current")
    else:
        current_data = (
            pd.concat(states_dfs_filtered)
            .loc[:, etl_cols] # Ensure only etl_cols are selected before drop_duplicates
            .drop_duplicates()
            .assign(dataset="current")
        )  # Keep only the required columns
        # The following block was redundant and caused a NameError for 'states_dfs'
        # current_data = (
        #     pd.concat(states_dfs) 
        #     .loc[:, etl_cols]
        #     .drop_duplicates()
        #     .assign(dataset="current")
        # )  # Keep only the required columns

    #%%
    new_history = pd.concat([current_data, history], sort=False)  # Update the history])
    new_history = new_history.drop_duplicates(
        subset=etl_cols, keep="last"
    )  # Keep only the new records
    new_history.loc[
        lambda df: df["first_time_seen"].isnull(), "first_time_seen"
    ] = pd.Timestamp.now()
    new_history.loc[
        (new_history["dataset"] == "history") & (new_history["not_seen_since"].isnull()), "not_seen_since"
    ] = pd.Timestamp.now() # Only update not_seen_since if it's currently null and from history

    dates = ["not_seen_since", "first_time_seen"]
    for date in dates:
        new_history[date] = pd.to_datetime(new_history[date]).dt.date
    
    # Drop the 'dataset' helper column before saving
    new_history_to_save = new_history.drop(columns=['dataset'])
    
    new_history_to_save.loc[:, sorting_cols].sort_values(sorting_cols).to_csv(
        output_path, index=False # Save to the specified output_path
    )
    print(f"Arquivo atualizado com sucesso! {output_path}")
    #%%
    return new_history


# %%
if __name__ == "__main__":
    update_records()
