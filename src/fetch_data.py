import csv
import io
import os
import random
import time
from pathlib import Path

import pandas as pd
import ibis
import requests

from geocode_cnefe import cobertura, geocodificar
from utils import converter_valor_monetario_para_float, converter_percentual_para_float

INPUT_DIR = "data"
OUTPUT_DIR = "output_data"

DEFAULT_URL_BASE = "https://venda-imoveis.caixa.gov.br/listaweb/Lista_imoveis_{}.csv"

UFS = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS",
    "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC",
    "SE", "SP", "TO",
]

# Ordem das colunas gravadas em data/imoveis_<UF>.csv.
CSV_COLUMNS = [
    "link", "endereco", "bairro", "descricao", "preco", "avaliacao",
    "desconto", "modalidade", "foto", "cidade", "estado", "financiamento",
]

# Cabeçalho da Caixa -> coluna do nosso CSV.
CAIXA_COLUMNS = {
    "N° do imóvel": "link",
    "UF": "estado",
    "Cidade": "cidade",
    "Bairro": "bairro",
    "Endereço": "endereco",
    "Preço": "preco",
    "Valor de avaliação": "avaliacao",
    "Desconto": "desconto",
    "Financiamento": "financiamento",
    "Descrição": "descricao",
    "Modalidade de venda": "modalidade",
    "Link de acesso": "link_acesso",
}


# O anti-bot da Caixa avalia coerência, não só o User-Agent. Um UA de navegador
# sozinho, sem os cabeçalhos que o navegador manda junto, é pior que nenhum:
# medido em 01/09/2026, UA isolado passou 0 de 6, sem header nenhum passou 3 de
# 10, e este conjunto coerente passou 10 de 10.
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "pt-BR,pt;q=0.8,en-US;q=0.5,en;q=0.3",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}


class FetchError(RuntimeError):
    """Falha ao baixar ou interpretar a lista de imóveis de um estado."""


class BlockedError(FetchError):
    """O anti-bot da Caixa respondeu no lugar do CSV."""


def _is_block_page(content):
    """Reconhece a página do Radware Bot Manager, servida com HTTP 200."""
    head = content[:2048].lower()
    return b"bot manager" in head or b"<html" in head or b"<head>" in head


def parse_caixa_csv(content):
    """Converte o CSV da Caixa (latin-1, ';', duas linhas de preâmbulo) em DataFrame.

    Aceita bytes ou str. Retorna um DataFrame com as colunas de CSV_COLUMNS.
    """
    if isinstance(content, bytes):
        content = content.decode("latin-1")

    rows = list(csv.reader(io.StringIO(content), delimiter=";"))
    header_index = None
    for index, row in enumerate(rows):
        normalized = [cell.strip() for cell in row]
        if "N° do imóvel" in normalized or "Nº do imóvel" in normalized:
            header_index = index
            break

    if header_index is None:
        raise FetchError("Cabeçalho da lista de imóveis não encontrado no CSV da Caixa.")

    header = [cell.strip() for cell in rows[header_index]]
    header = ["N° do imóvel" if cell == "Nº do imóvel" else cell for cell in header]
    records = []
    for row in rows[header_index + 1:]:
        if not any(cell.strip() for cell in row):
            continue
        record = dict(zip(header, row))
        records.append({
            CAIXA_COLUMNS[name]: value.strip()
            for name, value in record.items()
            if name in CAIXA_COLUMNS
        })

    df = pd.DataFrame(records)
    if df.empty:
        return pd.DataFrame(columns=CSV_COLUMNS)

    df["preco"] = df["preco"].map(converter_valor_monetario_para_float)
    df["avaliacao"] = df["avaliacao"].map(converter_valor_monetario_para_float)
    df["desconto"] = df["desconto"].map(converter_percentual_para_float)
    df["foto"] = ""
    for column in CSV_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA

    return df[CSV_COLUMNS]


def fetch_state(uf, url_base=None, session=None, timeout=60, jitter=(0.8, 2.5), raw_dir=None):
    """Baixa e interpreta a lista de imóveis de um estado, uma tentativa.

    Cada chamada usa uma sessão HTTP nova de propósito. O anti-bot da Caixa
    marca o cliente pelos cookies `__uzm*` que injeta na primeira resposta;
    reenviá-los identifica quem já foi avaliado e o bloqueio passa a ser quase
    certo. Medido em 01/09/2026: reusando a sessão, 1 de 8 estados passou; com
    sessão limpa por requisição, 6 de 8.

    Levanta BlockedError quando o anti-bot responde no lugar do CSV. Insistir
    no mesmo estado não ajuda — quem chama deve seguir para o próximo e voltar
    a este numa rodada seguinte, que é o que fetch_all_states faz.
    """
    url_base = url_base or os.getenv("URL_BASE") or DEFAULT_URL_BASE
    getter = session.get if session is not None else requests.Session().get
    response = getter(url_base.format(uf), timeout=timeout, headers=BROWSER_HEADERS)

    if response.status_code != 200:
        raise FetchError(f"{uf}: a Caixa respondeu HTTP {response.status_code}.")

    if _is_block_page(response.content):
        raise BlockedError(f"{uf}: o anti-bot da Caixa respondeu no lugar do CSV.")

    df = parse_caixa_csv(response.content)
    if df.empty:
        raise FetchError(f"{uf}: a Caixa não retornou nenhum imóvel.")

    # O CSV original é a fonte primária e desaparece quando a Caixa atualiza a
    # lista. O Parquet publicado é derivado; só o bruto preserva o cabeçalho
    # com a data de geração e o texto exatamente como a Caixa escreveu.
    if raw_dir is not None:
        destino = Path(raw_dir)
        destino.mkdir(parents=True, exist_ok=True)
        (destino / f"Lista_imoveis_{uf}.csv").write_bytes(response.content)

    if jitter:
        time.sleep(random.uniform(*jitter))
    return df


def fetch_all_states(
    url_base=None,
    input_dir=None,
    ufs=None,
    rodadas=4,
    espera_entre_rodadas=20.0,
    raw_dir=None,
):
    """Baixa todos os estados e reescreve os CSVs em `data/`.

    Percorre os estados pendentes em rodadas: um bloqueio não interrompe a
    passagem, o estado volta para a fila e é tentado de novo depois. Insistir
    no mesmo estado em sequência é o que o anti-bot pune — medido no runner do
    GitHub em 01/09/2026, seis tentativas seguidas em AL falharam todas,
    enquanto rodadas sobre os pendentes fecharam 27 de 27.

    Só grava depois de obter todos os estados: uma falha isolada não deixa
    `data/` com um recorte parcial da Caixa.
    """
    pendentes = list(ufs or UFS)
    frames = {}

    for rodada in range(1, rodadas + 1):
        if not pendentes:
            break
        print(f"Rodada {rodada}: {len(pendentes)} estado(s) a baixar.")
        falharam = []
        for uf in pendentes:
            try:
                frames[uf] = fetch_state(uf, url_base=url_base, raw_dir=raw_dir)
            except BlockedError:
                falharam.append(uf)
        pendentes = falharam
        if pendentes:
            print(f"  bloqueados nesta rodada: {', '.join(pendentes)}")
            if rodada < rodadas:
                time.sleep(espera_entre_rodadas)

    if pendentes:
        raise BlockedError(
            f"o anti-bot da Caixa bloqueou {len(pendentes)} estado(s) em "
            f"{rodadas} rodadas: {', '.join(pendentes)}. Nada foi gravado."
        )

    input_path = Path(input_dir or INPUT_DIR)
    input_path.mkdir(parents=True, exist_ok=True)
    for uf, df in frames.items():
        df.to_csv(input_path / f"imoveis_{uf}.csv", index=False)
        print(f"{uf}: {len(df)} imóveis gravados.")

    return frames


def process_local_data():
    """
    Processes local CSV files, unnions them, transforms with Ibis,
    geocodes, and saves as a single Parquet file.
    """
    input_path = Path(INPUT_DIR)
    output_path = Path(OUTPUT_DIR)
    output_path.mkdir(parents=True, exist_ok=True)

    csv_files = list(input_path.glob("imoveis_*.csv"))

    if not csv_files:
        raise FileNotFoundError(
            f"Nenhum arquivo CSV encontrado em {INPUT_DIR}; "
            "processamento solicitado não pode reutilizar artefato anterior."
        )

    conn = ibis.duckdb.connect()

    # Load all CSVs into a single table
    all_tables = []
    for csv_file in csv_files:
        table_name = f"imoveis_{csv_file.stem.split('_')[1]}"
        df = pd.read_csv(csv_file)
        # Ensure 'foto' and 'bairro' columns are string type
        if 'foto' in df.columns:
            df['foto'] = df['foto'].astype(str)
        if 'bairro' in df.columns:
            df['bairro'] = df['bairro'].astype(str)
        conn.create_table(table_name, df, overwrite=True)
        all_tables.append(conn.table(table_name))

    # Union all tables
    imoveis_table = ibis.union(*all_tables)

    # Basic transformations
    imoveis_table = imoveis_table.mutate(bairro=imoveis_table.bairro.fill_null("").upper().strip())
    imoveis_table = imoveis_table.drop_null('link')
    imoveis_table = imoveis_table.distinct()

    # Geocodificação por CNEFE: um join em DuckDB, sem rede no caminho
    # crítico. Ver src/geocode_cnefe.py e knowledge/concepts/geocodificacao.md.
    df = imoveis_table.to_pandas()

    if 'latitude' not in df.columns:
        df['latitude'] = pd.NA
    if 'longitude' not in df.columns:
        df['longitude'] = pd.NA
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

    pendentes = int(df['latitude'].isnull().sum())
    if pendentes:
        print(f"Geocodificando {pendentes} endereços pelo CNEFE...")
        df = geocodificar(df)
        for nivel, quantos in cobertura(df).items():
            print(f"  {nivel}: {quantos}")

    # Save as a single Parquet file
    output_file = output_path / "imoveis_geocoded.parquet"
    df.to_parquet(output_file, index=False)
    print(f"Salvo dados processados para {output_file}")

if __name__ == "__main__":
    process_local_data()
