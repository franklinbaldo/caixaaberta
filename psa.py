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
    file = Path(output, makedirs=True)  # Create the output file
    history = pd.read_csv(file)[cols].assign(dataset="history")  # Get the history
    states_files = Path("data/").glob("imoveis_*.csv")  # Get the states files
    states_dfs = (pd.read_csv(csv) for csv in states_files)  # Read the states files
    current_data = (
        pd.concat(states_dfs)
        .loc[:, etl_cols]
        .drop_duplicates()
        .assign(dataset="current")
    )  # Keep only the required columns

    #%%
    new_history = pd.concat([current_data, history], sort=False)  # Update the history])
    new_history = new_history.drop_duplicates(
        subset=etl_cols, keep="last"
    )  # Keep only the new records
    new_history.loc[
        lambda df: df["first_time_seen"].isnull(), "first_time_seen"
    ] = pd.Timestamp.now()
    new_history.loc[
        lambda df: df.dataset == "history", "not_seen_since"
    ] = pd.Timestamp.now()

    dates = ["not_seen_since", "first_time_seen"]
    for date in dates:
        new_history[date] = pd.to_datetime(new_history[date]).dt.date
    new_history.loc[:, sorting_cols].sort_values(sorting_cols).to_csv(file_path, index=False)
    print("Arquivo atualizado com sucesso!", file_path)
    #%%
    return new_history


# %%
if __name__ == "__main__":
    update_records()
