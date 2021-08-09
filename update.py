#%%

from os import makedirs
import pandas as pd
from pathlib import Path
from etl import log
from etl import cols as etl_cols
from pandas.core.frame import DataFrame

cols = etl_cols + [
    "first_time_seen",
    "not_seen_since"
]

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
    file = Path(output, makedirs=True)
    try:
        old_df = pd.read_csv(file)
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
    new_df.loc[new_df["fist_time_seen"].isnull(), "fist_time_seen"] = pd.Timestamp()

    removed_df = old_df.loc[~(old_df.index.isin(new_df.index))]
    removed_df[removed_df["not_seen_since"].isnull(), "not_seen_since"] = pd.Timestamp()

    concated_df = (
        pd.concat([new_df, removed_df]).sort_index().sort_values(["estado", "cidade"])
    )

    if file_path is not None:
        concated_df.reset_index()[cols].to_csv(file_path, index=False)
        print("Arquivo atualizado com sucesso!", file_path)

    return concated_df


if __name__ == "__main__":
    update_imoveis()
