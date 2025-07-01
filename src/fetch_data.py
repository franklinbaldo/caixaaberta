import os
from pathlib import Path
import pandas as pd
import requests
from lxml import html
from dotenv import load_dotenv

# Constants
BASE_DETALHE_URL = "https://venda-imoveis.caixa.gov.br/sistema/detalhe-imovel.asp?hdnOrigem=index&hdnimovel="
OUTPUT_CSV_TEMPLATE = "dbt_real_estate/seeds/imoveis_{}.csv"  # Updated path
SCRAPING_COLS = [
    "link", "endereco", "bairro", "descricao", "preco", "avaliacao",
    "desconto", "modalidade", "foto", "cidade", "estado",
]
BRAZILIAN_STATES = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS",
    "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC",
    "SE", "SP", "TO",
]

def _log_download_success(state: str, df_count: int) -> None:
    if df_count > 0:
        print(f"[DOWNLOAD] Sucesso para o estado {state}: {df_count} registros baixados.")
    else:
        print(f"[DOWNLOAD] Aviso para o estado {state}: Nenhum registro baixado.")

def _extract_data_for_state(state: str, base_url_fmt_str: str) -> pd.DataFrame:
    if not base_url_fmt_str:
        print("Erro: URL_BASE não configurada. Verifique seu arquivo .env e a variável URL_BASE.")
        return pd.DataFrame(columns=SCRAPING_COLS)

    url = base_url_fmt_str.format(state)
    print(f"Baixando dados para o estado: {state} de {url}")
    try:
        reqs = requests.get(url, timeout=30) # Added timeout
        reqs.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
    except requests.exceptions.RequestException as e:
        print(f"Erro de requisição para {state} em {url}: {e}")
        return pd.DataFrame(columns=SCRAPING_COLS)

    try:
        # Using the first table found by read_html
        # Adding error_bad_lines=False and warn_bad_lines=True as pd.read_html can be strict
        # However, these are not valid parameters for read_html.
        # Instead, we'll rely on the try-except for parsing issues.
        tables = pd.read_html(reqs.text, header=0)
        if not tables:
            print(f"Nenhuma tabela encontrada para o estado {state}.")
            return pd.DataFrame(columns=SCRAPING_COLS)
        extracted_df = tables[0]
    except ValueError:  # Handles cases where no table is found or table is empty
        print(f"Nenhuma tabela encontrada ou tabela vazia para o estado {state} após parsing.")
        return pd.DataFrame(columns=SCRAPING_COLS)
    except Exception as e: # Catch other potential parsing errors
        print(f"Erro ao fazer parse do HTML para o estado {state}: {e}")
        return pd.DataFrame(columns=SCRAPING_COLS)


    if extracted_df.empty:
        return pd.DataFrame(columns=SCRAPING_COLS)

    extracted_df.columns = SCRAPING_COLS[:len(extracted_df.columns)] # Assign standard column names, handle if less columns than expected

    # Ensure all SCRAPING_COLS are present, fill with NA if not
    for col in SCRAPING_COLS:
        if col not in extracted_df.columns:
            extracted_df[col] = pd.NA


    # Extracting links
    try:
        tree = html.fromstring(reqs.content)
        # Adjusted XPath to be more robust, looking for links within table cells that contain the specific href structure
        link_elements = tree.xpath("//table[contains(@class, 'responsive')]/tbody/tr/td/a[contains(@href, 'detalhe-imovel.asp')]")
        links = [
            str(link.get("href")).replace(BASE_DETALHE_URL, "").strip()
            for link in link_elements
        ]

        # Check if number of links matches number of rows. If not, this indicates a potential misalignment.
        # This part is tricky because the links might not always be in the first column or might be missing.
        # For now, we'll assign if counts match, otherwise, we might need a more sophisticated row-to-link mapping.
        if len(links) == len(extracted_df):
            extracted_df["link"] = links
        elif not links and 'link' in extracted_df.columns and extracted_df['link'].isna().all():
             print(f"Aviso: Nenhum link extraído via XPath para {state}, e coluna 'link' do read_html está vazia. 'link' permanecerá NA.")
        elif links : # If some links were extracted but count doesn't match
            print(f"Aviso: Discrepância no número de links ({len(links)}) e linhas ({len(extracted_df)}) para {state}. A coluna 'link' pode estar incorreta.")
            # As a fallback, if 'link' column from read_html is entirely empty, use extracted links if only one row.
            # This is a heuristic and might not be perfect.
            if 'link' in extracted_df.columns and extracted_df['link'].isna().all() and len(extracted_df) == 1 and len(links) == 1:
                 extracted_df["link"] = links
            elif 'link' not in extracted_df.columns: # if link column doesn't exist at all
                 extracted_df["link"] = pd.NA # create it as NA
            # else, keep the 'link' column from read_html if it has data, or it will be NA

    except Exception as e:
        print(f"Erro ao extrair links via XPath para {state}: {e}. A coluna 'link' pode estar incompleta ou ausente.")
        if "link" not in extracted_df.columns:
             extracted_df["link"] = pd.NA


    # Monetary conversion
    for col_name in ["preco", "avaliacao"]:
        if col_name in extracted_df.columns:
            extracted_df[col_name] = (
                extracted_df[col_name]
                .astype(str)
                .str.replace("R$", "", regex=False)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
                .pipe(lambda x: pd.to_numeric(x, errors='coerce'))
            )
    return extracted_df

def _transform_state_data(df: pd.DataFrame) -> pd.DataFrame:
    print("Transformando dados do estado...")
    if df.empty:
        return df

    if 'bairro' in df.columns:
        df["bairro"] = df["bairro"].fillna("").astype(str).str.upper().str.strip()

    # Ensure all necessary columns for subset exist
    subset_cols = [col for col in ["estado", "cidade", "link"] if col in df.columns]
    if not subset_cols or df['link'].isna().all(): # Cannot drop duplicates if key columns are missing or link is all NA
        print("Aviso: Colunas chave (especialmente 'link') para deduplicação não encontradas ou 'link' está todo NA. Pulando deduplicação.")
        return df

    # Drop rows where 'link' is NA before sorting and dropping duplicates, as NA links are not useful unique identifiers
    df.dropna(subset=['link'], inplace=True)
    if df.empty:
        print("DataFrame vazio após remover linhas com 'link' NA.")
        return df

    df = df.sort_values(by=subset_cols)
    df = df.drop_duplicates(subset=subset_cols, keep="first")
    return df

def _load_state_data(df: pd.DataFrame, state: str) -> None:
    if df.empty:
        print(f"Nenhum dado para carregar para o estado {state}.")
        return

    # Ensure the target directory exists
    output_file_path = Path(OUTPUT_CSV_TEMPLATE.format(state))
    output_file_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Carregando dados para {output_file_path}")
    df.to_csv(output_file_path, index=False, encoding='utf-8') # Added encoding

def baixar_csvs_por_estado(state: str, base_url_fmt_str: str) -> None:
    print(f"Processando estado: {state}")
    extracted_df = _extract_data_for_state(state, base_url_fmt_str)
    if not extracted_df.empty:
        transformed_df = _transform_state_data(extracted_df)
        _load_state_data(transformed_df, state)
        _log_download_success(state, len(transformed_df))
    else:
        _log_download_success(state, 0)
        print(f"Nenhum dado extraído para o estado {state}.")

def fetch_all_data() -> None:
    print("Iniciando download de CSVs para todos os estados...")
    load_dotenv() # Load .env variables, like URL_BASE
    base_url_fmt_str = os.getenv("URL_BASE")

    if not base_url_fmt_str:
        print("Erro fatal: URL_BASE não fornecida. Não é possível continuar com o download dos dados.")
        return

    # Ensure dbt_real_estate/seeds directory exists
    # The _load_state_data function now handles this for each file,
    # but it's good practice to ensure the root seeds dir exists if many files are created.
    Path("dbt_real_estate/seeds/").mkdir(parents=True, exist_ok=True)

    for state in BRAZILIAN_STATES:
        try:
            baixar_csvs_por_estado(state, base_url_fmt_str)
        except Exception as e:
            print(f"Erro crítico ao processar o estado {state}: {e}")

if __name__ == "__main__":
    fetch_all_data()
