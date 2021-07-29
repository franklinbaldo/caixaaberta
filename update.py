#%%

from os import makedirs
import pandas as pd
from pathlib import Path

from etl import cols as etl_cols
from pandas.core.frame import DataFrame

cols = etl_cols + [
    "first_time_seen",
]

file_path = "imoveis_BR.csv"


def update_imoveis(file_path="imoveis_BR.csv"):
    file = Path(file_path, makedirs=True)
    try:
        old_df = pd.read_csv(Path("imoveis_BR.csv"))
    except:
        old_df = pd.DataFrame(columns=cols)
    old_df = old_df.set_index("link")

    new_df = pd.concat(
        [
            pd.read_csv(file).set_index("link")
            for file in Path("data/").glob("imoveis_*.csv")
        ]
    )
    new_df["fist_time_seen"] = None
    new_df = new_df.combine_first(old_df.loc[old_df.index.isin(new_df.index)])
    new_df["fist_time_seen"] = new_df["fist_time_seen"].fillna(value=new_df["data"])

    removed_df = old_df.loc[~(old_df.index.isin(new_df.index))]

    concated_df = (
        pd.concat([new_df, removed_df]).sort_index().sort_values(["estado", "cidade"])
    )
    concated_df['data'] = pd.to_datetime(concated_df['data']).dt.date
    concated_df['first_time_seen'] = pd.to_datetime(concated_df['first_time_seen']).dt.date

    concated_df.to_csv(file_path)

    if file_path is not None:
        concated_df.to_csv(file_path)
        print("Arquivo atualizado com sucesso!", file_path)

    return concated_df


if __name__ == "__main__":
    update_imoveis()
