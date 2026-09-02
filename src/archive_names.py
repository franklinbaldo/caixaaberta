"""Nomes e identificadores da publicação no Internet Archive.

O acervo da Caixa é um retrato diário: imóvel vendido some da lista e a fonte
não guarda histórico. Por isso cada publicação vira um arquivo próprio, datado
e imutável — sobrescrever um nome fixo destruiria exatamente a série temporal
que este projeto existe para preservar.

Os retratos se acumulam em um item por ano, para nenhum item crescer sem
limite. Não existe nome estável apontando para o mais recente: o nome do dia é
calculado a partir da data, tanto aqui quanto no DDL, que monta a URL em SQL.
"""

from datetime import date

ITEM_PREFIX = "imoveis-caixa-economica-federal"
PARQUET_PREFIX = "imoveis_geocoded"
BRUTO_PREFIX = "imoveis_csv_bruto"


def item_do_ano(quando: date | None = None) -> str:
    """Identificador do item que recebe os retratos de um ano."""
    return f"{ITEM_PREFIX}-{(quando or date.today()).year}"


def parquet_datado(quando: date | None = None) -> str:
    return f"{PARQUET_PREFIX}_{(quando or date.today()).isoformat()}.parquet"


def bruto_datado(quando: date | None = None) -> str:
    return f"{BRUTO_PREFIX}_{(quando or date.today()).isoformat()}.zip"
