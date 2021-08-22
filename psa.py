#%%
# What is a Persistent Staging Area?
# Source system data is loaded into PSA without transformation
# Records are never deleted from PSA (archives may occur)
# PSA stores all unique records loaded (tracks history)
# Many more fields are stored in PSA than required by the data warehouse

from os import makedirs
from pathlib import Path

import pandas as pd
from pandas.core.frame import DataFrame

from etl import cols as etl_cols
from etl import log

cols = etl_cols + ["first_time_seen", "not_seen_since"]

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
    file = Path(output, makedirs=True)  # Create the output file
    history = pd.read_csv(file).loc[:, cols].set_index(etl_cols)  # Get the history
    states_files = Path("data/").glob("imoveis_*.csv")  # Get the states files
    states_dfs = (pd.read_csv(csv) for csv in states_files)  # Read the states files
    current_data = pd.concat(states_dfs)  # Concatenate the states files
    for col in cols:
        if col not in current_data.columns:
            current_data[col] = None
    current_data = current_data.set_index(etl_cols)  # Set the index
    current_data.update(history["first_time_seen"])
    current_data.update(history["not_seen_since"])
    new_records = lambda df: df["first_time_seen"].isnull()
    current_data.loc[new_records, "first_time_seen"] = pd.Timestamp.now()

    removed_data = history.loc[~current_data.index, :]
    removed_data.loc[:, "not_seen_since"] = pd.Timestamp.now()

    new_history = (
        pd.concat([current_data, removed_data])
        .drop_duplicates(keep="first")
        .sort_index()
        .reset_index()
    )
    dates = ["not_seen_since", "first_time_seen"]
    for date in dates:
        new_history[date] = pd.to_datetime(new_history[date]).dt.date
    new_history.to_csv(file_path, index=False)
    print("Arquivo atualizado com sucesso!", file_path)
    return new_history


if __name__ == "__main__":
    update_records()

# %%
