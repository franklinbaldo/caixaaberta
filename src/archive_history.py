"""Descobre e baixa o snapshot público imediatamente anterior no Archive.

O ponteiro ``latest.json`` só responde "qual é o último". Para derivar mudanças
com segurança precisamos responder uma pergunta diferente: qual é o maior
``scrape_date`` estritamente anterior à execução atual? Isso importa em
republicações do mesmo dia e na virada do ano.
"""

from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import requests

from archive_names import PARQUET_PREFIX, item_do_ano, url_no_item

_METADATA = "https://archive.org/metadata/{identifier}"
_PARQUET_RE = re.compile(
    rf"^{re.escape(PARQUET_PREFIX)}_(\d{{4}}-\d{{2}}-\d{{2}})\.parquet$"
)


@dataclass(frozen=True)
class SnapshotPublico:
    data: date
    item: str
    arquivo: str

    @property
    def url(self) -> str:
        return url_no_item(self.item, self.arquivo)


def _snapshots_no_item(identifier: str) -> list[SnapshotPublico]:
    url = _METADATA.format(identifier=identifier)
    try:
        resposta = requests.get(url, timeout=30)
    except requests.RequestException as exc:
        raise RuntimeError(f"Não foi possível ler o histórico em {url}: {exc}") from exc

    if resposta.status_code == 404:
        return []
    resposta.raise_for_status()

    snapshots = []
    for arquivo in resposta.json().get("files", []):
        nome = str(arquivo.get("name", ""))
        match = _PARQUET_RE.fullmatch(nome)
        if not match:
            continue
        snapshots.append(
            SnapshotPublico(
                data=date.fromisoformat(match.group(1)),
                item=identifier,
                arquivo=nome,
            )
        )
    return snapshots


def snapshot_anterior(quando: date) -> SnapshotPublico | None:
    """Maior snapshot público com data estritamente menor que ``quando``.

    O item do ano corrente basta na maior parte do calendário. Se ainda não há
    candidato nele, consultamos o ano anterior para cobrir 1º de janeiro e a
    primeira publicação de um ano sem depender de nomes globais ou listagens
    não delimitadas.
    """
    atual = [
        snapshot
        for snapshot in _snapshots_no_item(item_do_ano(quando))
        if snapshot.data < quando
    ]
    if atual:
        return max(atual, key=lambda snapshot: snapshot.data)

    ano_anterior = date(quando.year - 1, 12, 31)
    anteriores = [
        snapshot
        for snapshot in _snapshots_no_item(item_do_ano(ano_anterior))
        if snapshot.data < quando
    ]
    if not anteriores:
        return None
    return max(anteriores, key=lambda snapshot: snapshot.data)


def baixar_snapshot(snapshot: SnapshotPublico, destino: str | Path) -> Path:
    """Baixa um snapshot descoberto sem mantê-lo inteiro na memória."""
    caminho = Path(destino)
    caminho.parent.mkdir(parents=True, exist_ok=True)
    try:
        with requests.get(snapshot.url, stream=True, timeout=(30, 300)) as resposta:
            resposta.raise_for_status()
            with caminho.open("wb") as arquivo:
                shutil.copyfileobj(resposta.raw, arquivo)
    except requests.RequestException as exc:
        raise RuntimeError(
            f"Não foi possível baixar o snapshot anterior {snapshot.url}: {exc}"
        ) from exc
    return caminho
