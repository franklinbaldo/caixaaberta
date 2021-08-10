#%%
import pandas as pd
from pandas._libs.tslibs.timestamps import Timestamp
import requests
from bs4 import BeautifulSoup
import datetime

output_csv = "data/imoveis_{}.csv"
base_url = "https://venda-imoveis.caixa.gov.br/listaweb/Lista_imoveis_{}.htm"
cols = [
    "link",
    "endereco",
    "bairro",
    "descricao",
    "preco",
    "valor",
    "desconto",
    "modalidade",
    "foto",
    "cidade",
    "estado"
]
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

def log_sucess(state,transformed_df:pd.DataFrame) -> None:
    assert not transformed_df.empty, "Empty dataframe"
    df = pd.DataFrame([[state,datetime.datetime.now()]],columns=["state","date"])
    df.to_csv(log,mode='a',index=False,header=None)

def extract_state(state) -> pd.DataFrame:
    url = base_url.format(state)
    print("Extracting state: {} from {}".format(state, url))
    reqs = requests.get(url)
    soup = BeautifulSoup(reqs.text, "html.parser")
    extracted_df = pd.read_html(reqs.text, header=0)[0]

    extracted_df.columns = cols
    extracted_df["link"] = [
        str(link.get("href")).replace(base_detalhe_url, "").strip()
        for link in soup.find_all("a")
        if " Detalhes" in link.text
    ]
    extracted_df["preco"] = (
        extracted_df["preco"].astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .astype(float)
    )
    extracted_df["valor"] = (
        extracted_df["valor"].astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .astype(float)
    )
    
    return extracted_df


def transform(extracted_df) -> pd.DataFrame:
    print("Transforming")
    transformed_df = extracted_df
    transformed_df["bairro"] = transformed_df["bairro"].astype(str).str.upper().str.strip()
    transformed_df = transformed_df.sort_values(by=["estado", "cidade", "link"])
    transformed_df = transformed_df.drop_duplicates(
        subset=["estado", "cidade", "link"], keep="first"
    )
    return transformed_df


def load(transformed_df, output_csv) -> None:
    print("Loading")
    transformed_df.to_csv(output_csv, index=False)


def etl_state(state, output_csv=output_csv)->None:
    """
    Takes a state and extracts the data from the website
    and saves it to a csv file.
    state: string
    output_csv: string
    """
    df = extract_state(state)
    transformed_df = transform(df)
    load(transformed_df, output_csv.format(state))
    log_sucess(state,transformed_df)



def etl_many(states):
    for state in states:
        print(state)
        try:
            etl_state(state)
        except Exception as e:
            print(e)


# %%
if __name__ == "__main__":
    etl_many(brazilian_states)
