#%%
import datetime

import io # Added import
from lxml import html
import pandas as pd
import requests

output_csv = "data/imoveis_{}.csv"
base_url = "https://venda-imoveis.caixa.gov.br/listaweb/Lista_imoveis_{}.htm"


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


def log_sucess(state, transformed_df: pd.DataFrame) -> None:
    assert not transformed_df.empty, "Empty dataframe"
    df = pd.DataFrame([[state, datetime.datetime.now()]], columns=["state", "date"])
    df.to_csv(log, mode="a", index=False, header=None)


def extract_state(state) -> pd.DataFrame:
    url = base_url.format(state)
    print(f"Extracting state: {state} from {url}")
    reqs = requests.get(url)
    # Use io.StringIO to treat the string as a file
    html_content = io.StringIO(reqs.text)
    tree = html.parse(html_content)
    
    # Reset html_content offset to allow reading again by pd.read_html
    html_content.seek(0)

    # Define final columns early for use in try-except and final selection
    final_columns = ['cidade', 'endereco', 'bairro', 'descricao', 'preco', 'avaliacao', 'link', 'estado']

    try:
        # Try to read the HTML table
        extracted_df = pd.read_html(html_content, header=0)[0]
        if extracted_df.empty and not list(extracted_df.columns): # Handles table with 0 rows and 0 columns
             raise ValueError("Table resulted in empty DataFrame with no columns.")
    except (ValueError, IndexError) as e:
        # This block catches:
        # 1. ValueError from pd.read_html if no tables are found.
        # 2. IndexError if pd.read_html returns an empty list (no tables).
        # 3. ValueError explicitly raised if table is empty with no columns (rare).
        print(f"No table found or table is empty for state {state}. Error: {e}. Returning empty DataFrame structured with final_columns.")
        extracted_df = pd.DataFrame(columns=final_columns)
        # Ensure correct dtypes for the empty DataFrame
        for col_name in final_columns:
            if col_name in ['preco', 'avaliacao']:
                extracted_df[col_name] = pd.Series(dtype=float)
            else:
                extracted_df[col_name] = pd.Series(dtype=object)
        # estado column would be all 'object' if not explicitly handled, but it's fine.
        extracted_df['estado'] = state # Assign state even for empty/error cases
        return extracted_df

    # Rename columns from Portuguese (as found in example HTML) to standard names
    # This mapping should be adjusted if the actual website's column names differ
    rename_map = {
        "Cidade": "cidade",
        "Endereço": "endereco",
        "Bairro": "bairro",
        "Descrição": "descricao",
        "Preço": "preco",
        "Avaliação": "avaliacao",
        # The 'Link' column from HTML usually contains just text, not the href itself.
        # We'll overwrite it with the extracted link.
        "Link": "link_html_text" # Temporary name for the original Link column from HTML
    }
    extracted_df.rename(columns=rename_map, inplace=True)

    # Extract actual links using xpath.
    # For the test HTML, links are like <td><a href="...">Link</a></td>
    # Using a more specific XPath to get links from table cells, assuming they have an href.
    link_elements = tree.xpath("//table[@class='Caixa']//td/a[@href]")
    links = [
        str(link.get("href")).replace(base_detalhe_url, "").strip()
        for link in link_elements
    ]

    # Assign links if the number matches DataFrame length, otherwise fill with NA
    if len(extracted_df) > 0: # Process links only if there are data rows
        if len(links) == len(extracted_df):
            extracted_df["link"] = links
        else:
            print(f"Warning: Link count mismatch or no links found for state {state}. Rows: {len(extracted_df)}, Links: {len(links)}. Setting 'link' to NA.")
            extracted_df["link"] = pd.NA # Use pd.NA for missing/mismatched links
    elif 'link' not in extracted_df.columns: # Ensure 'link' column exists even for 0-row df from pd.read_html
        extracted_df['link'] = pd.Series(dtype='object')


    # Clean 'preco' and 'avaliacao' columns
    # Ensure these columns exist before trying to clean them.
    # pd.read_html should have created them if they were in the table.
    # Our rename_map ensures they are named 'preco' and 'avaliacao'.
    if "preco" in extracted_df.columns:
        extracted_df["preco"] = (
            extracted_df["preco"]
            .astype(str)
            .str.replace("R$", "", regex=False) # Remove R$
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
            .astype(float)
        )
    else:
        extracted_df["preco"] = pd.NA # Or some other default like 0.0

    if "avaliacao" in extracted_df.columns:
        extracted_df["avaliacao"] = (
            extracted_df["avaliacao"]
            .astype(str)
            .str.replace("R$", "", regex=False) # Remove R$
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
            .astype(float)
        )
    else:
        extracted_df["avaliacao"] = pd.NA # Or some other default like 0.0


    # Add 'estado' column
    extracted_df["estado"] = state
    
    # Select and order columns to match the desired output schema for this function
    # The test expects: ['cidade', 'endereco', 'bairro', 'descricao', 'preco', 'avaliacao', 'link', 'estado']
    # final_columns was defined at the start of the function.
    
    # Ensure all final columns exist, filling with pd.NA if created new, and ensure correct dtypes
    for col_name in final_columns:
        if col_name not in extracted_df.columns:
            # Assign series with specific dtype to ensure correct type even if all NA
            if col_name in ['preco', 'avaliacao']:
                extracted_df[col_name] = pd.Series(dtype=float)
            else: # 'cidade', 'endereco', 'bairro', 'descricao', 'link', 'estado'
                extracted_df[col_name] = pd.Series(dtype=object)
    
    # Final dtype enforcement for key columns
    if 'link' in extracted_df.columns:
        extracted_df['link'] = extracted_df['link'].astype(object)
    if 'bairro' in extracted_df.columns:
        extracted_df['bairro'] = extracted_df['bairro'].astype(object)
    if 'preco' in extracted_df.columns:
        extracted_df['preco'] = extracted_df['preco'].astype(float)
    if 'avaliacao' in extracted_df.columns:
        extracted_df['avaliacao'] = extracted_df['avaliacao'].astype(float)
    if 'estado' in extracted_df.columns: # estado should also be object
        extracted_df['estado'] = extracted_df['estado'].astype(object)
            
    return extracted_df[final_columns]


def transform(extracted_df) -> pd.DataFrame:
    print("Transforming")
    transformed_df = extracted_df
    transformed_df["bairro"] = (
        transformed_df["bairro"].fillna("").astype(str).str.upper().str.strip()
    )
    transformed_df = transformed_df.sort_values(by=["estado", "cidade", "link"])
    transformed_df = transformed_df.drop_duplicates(
        subset=["estado", "cidade", "link"], keep="first"
    )
    return transformed_df


def load(transformed_df, output_csv) -> None:
    print("Loading")
    transformed_df.to_csv(output_csv, index=False)


def etl_state(state, output_csv=output_csv) -> None:
    """
    Takes a state and extracts the data from the website
    and saves it to a csv file.
    state: string
    output_csv: string
    """
    df = extract_state(state)
    transformed_df = transform(df)
    load(transformed_df, output_csv.format(state))
    log_sucess(state, transformed_df)


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
