"""Nomes, identificadores e a data de uma publicação no Internet Archive.

O acervo da Caixa é um retrato diário: imóvel vendido some da lista e a fonte
não guarda histórico. Por isso cada publicação vira um arquivo próprio, datado
e imutável — sobrescrever um nome fixo destruiria exatamente a série temporal
que este projeto existe para preservar. Os retratos se acumulam em um item por
ano, para nenhum item crescer sem limite.

A data é propriedade da execução, não de cada chamada: `data_de_publicacao()`
é lida **uma vez** no início do pipeline e passada adiante. Se cada nome
consultasse o relógio por conta própria, uma execução atravessando a meia-noite
poderia gravar o zip em D e o Parquet em D+1 — e, na virada do ano, o arquivo
num item e o identificador em outro.

O fuso é UTC, sempre. O produtor roda em runner UTC e o consumidor pode estar
em qualquer lugar; sem um fuso canônico, "hoje" difere entre os dois por horas
ao redor da meia-noite e por um item inteiro na virada do ano.
"""

from datetime import UTC, date, datetime

ITEM_PREFIX = "imoveis-caixa-economica-federal"
PARQUET_PREFIX = "imoveis_geocoded"
BRUTO_PREFIX = "imoveis_csv_bruto"

# O item sem ano não guarda dado: guarda o ponteiro para o último retrato
# efetivamente publicado. O calendário não serve como ponteiro — a publicação
# do dia pode falhar, e aí "hoje" aponta para um arquivo que não existe.
ITEM_PONTEIRO = ITEM_PREFIX
MANIFESTO = "latest.json"


def data_de_publicacao() -> date:
    """A data de hoje em UTC, o único fuso em que produtor e consumidor batem."""
    return datetime.now(UTC).date()


def item_do_ano(quando: date) -> str:
    """Identificador do item que recebe os retratos de um ano."""
    return f"{ITEM_PREFIX}-{quando.year}"


def parquet_datado(quando: date) -> str:
    return f"{PARQUET_PREFIX}_{quando.isoformat()}.parquet"


def bruto_datado(quando: date) -> str:
    return f"{BRUTO_PREFIX}_{quando.isoformat()}.zip"


def url_no_archive(quando: date, arquivo: str) -> str:
    return f"https://archive.org/download/{item_do_ano(quando)}/{arquivo}"


def url_do_manifesto() -> str:
    return f"https://archive.org/download/{ITEM_PONTEIRO}/{MANIFESTO}"
