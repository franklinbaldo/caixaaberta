#%%

from os import makedirs
import pandas as pd
from pathlib import Path
from etl import log
from etl import cols as etl_cols
from pandas.core.frame import DataFrame

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


def update_imoveis(output="imoveis_BR.csv"):
    """
    Updates imoveis.csv file.
    """
    file = Path(output, makedirs=True)
    history = pd.DataFrame(columns=cols)
    try:
        history = history.appen(pd.read_csv(file))
    except:
        pass
    for col in cols:
        if not col in history.columns:
            history.loc[:, col] = None
    unwnated_cols = [col for col in history.columns if col not in cols]
    history = history.drop(columns=unwnated_cols)
    history = history.set_index("link")

    states_files = Path("data/").glob("imoveis_*.csv")
    states_dfs = [ pd.read_csv(csv).set_index("link")  for csv in states_files ]

    current_data = pd.concat(states_dfs)
    current_data["first_time_seen"] = None
    current_data["not_seen_since"] = None
    current_data.update(history['first_time_seen'])
    null_first_time_seen = lambda df: df["first_time_seen"].isnull()
    current_data.loc[null_first_time_seen, "first_time_seen"] = pd.Timestamp.now()
    
    not_in_curent_data = lambda df: ~(df.index.isin(current_data.index))
    removed_data = history.loc[not_in_curent_data]
    null_not_seen = lambda df: df["not_seen_since"].isnull()
    removed_data.loc[null_not_seen, "not_seen_since"] = pd.Timestamp.now()

    new_history = (
        pd.concat([current_data, removed_data])
        .sort_index()
        .sort_values(["estado", "cidade"])
    )
    dates = ["not_seen_since", "first_time_seen"]
    for date in dates:
        new_history[date] = pd.to_datetime(new_history[date]).dt.date
    if file_path is not None:
        cols_to_save = [col for col in cols if col in new_history]
        new_history[cols_to_save].to_csv(file_path)
        print("Arquivo atualizado com sucesso!", file_path)
    return new_history


if __name__ == "__main__":
    update_imoveis()

# %%
