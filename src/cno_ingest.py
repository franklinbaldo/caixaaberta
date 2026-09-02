"""Ingestão do snapshot aberto do Cadastro Nacional de Obras (CNO).

A Receita publica o CNO como recurso em massa no Portal Brasileiro de Dados
Abertos. O endereço físico do arquivo pode mudar; por isso a descoberta parte
do catálogo público e só depois baixa o recurso oficial.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
import zipfile
from collections.abc import Iterator
from pathlib import Path
from urllib.parse import urlparse

import requests

CATALOG_API = "https://dados.gov.br/dados/api/publico/conjuntos-dados"
DATASET_NAME = "Cadastro Nacional de Obras - CNO"
DATASET_PAGE = "https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-de-obras-cno"
DEFAULT_DIR = Path("cno_data/raw")
EXPECTED_FILES = (
    "cno.csv",
    "cno_areas.csv",
    "cno_cnaes.csv",
    "cno_vinculos.csv",
    "cno_totais.csv",
)


class CNOIngestError(RuntimeError):
    """O snapshot CNO não pôde ser descoberto, baixado ou validado."""


def _dicts(value: object) -> Iterator[dict]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _dicts(child)


def _texto(obj: dict) -> str:
    return " ".join(str(value) for value in obj.values() if isinstance(value, str)).lower()


def _urls(obj: dict) -> Iterator[str]:
    for key, value in obj.items():
        if not isinstance(value, str):
            continue
        if key.lower() not in {"url", "link", "uri", "endereco", "urlrecurso", "linkacesso"}:
            continue
        parsed = urlparse(value)
        if parsed.scheme in {"http", "https"} and parsed.netloc:
            yield value


def _dataset_id(payload: object) -> str | None:
    for obj in _dicts(payload):
        texto = _texto(obj)
        if "cadastro nacional de obras" not in texto:
            continue
        for key in ("id", "idConjuntoDados", "id_conjunto_dados", "codigo"):
            value = obj.get(key)
            if isinstance(value, (str, int)) and str(value).strip():
                return str(value)
    return None


def _resource_url(payload: object) -> str | None:
    candidatos: list[tuple[int, str]] = []
    for obj in _dicts(payload):
        texto = _texto(obj)
        for url in _urls(obj):
            lowered = url.lower()
            if lowered.endswith(".pdf") or "cno-metadados" in lowered:
                continue
            score = 0
            if "cadastro nacional de obras" in texto or " cno" in f" {texto}":
                score += 5
            if "csv" in texto:
                score += 3
            if "zip" in texto or lowered.endswith(".zip"):
                score += 2
            if "receita" in lowered or "gov.br" in lowered:
                score += 1
            if url.rstrip("/") == DATASET_PAGE.rstrip("/"):
                score -= 10
            candidatos.append((score, url))
    if not candidatos:
        return None
    score, url = max(candidatos, key=lambda item: item[0])
    return url if score > 0 else None


def discover_source_url(session=requests, timeout: int = 30) -> str:
    """Descobre no catálogo público a URL corrente do recurso de dados do CNO."""
    response = session.get(
        CATALOG_API,
        params={
            "isPrivado": "false",
            "pagina": 1,
            "nomeConjuntoDados": DATASET_NAME,
        },
        timeout=timeout,
        headers={"Accept": "application/json"},
    )
    response.raise_for_status()
    payload = response.json()

    direct = _resource_url(payload)
    if direct:
        return direct

    dataset_id = _dataset_id(payload)
    if not dataset_id:
        raise CNOIngestError(
            "O catálogo do dados.gov.br não retornou o conjunto do CNO. "
            f"Confira {DATASET_PAGE}."
        )

    detail = session.get(
        f"{CATALOG_API}/{dataset_id}",
        timeout=timeout,
        headers={"Accept": "application/json"},
    )
    detail.raise_for_status()
    source_url = _resource_url(detail.json())
    if not source_url:
        raise CNOIngestError(
            "O conjunto CNO foi encontrado, mas nenhum recurso de dados utilizável apareceu. "
            f"Confira {DATASET_PAGE}."
        )
    return source_url


def _member_map(archive: zipfile.ZipFile) -> dict[str, str]:
    members: dict[str, str] = {}
    for name in archive.namelist():
        basename = Path(name).name.lower()
        if basename:
            members[basename] = name
    return members


def validate_archive(path: Path | str) -> dict[str, str]:
    """Valida ZIP e devolve nome canônico -> membro real."""
    path = Path(path)
    if not zipfile.is_zipfile(path):
        raise CNOIngestError("O recurso do CNO não é um ZIP válido.")
    with zipfile.ZipFile(path) as archive:
        members = _member_map(archive)
        missing = [name for name in EXPECTED_FILES if name not in members]
        if missing:
            raise CNOIngestError(
                "Snapshot CNO incompleto; faltam: " + ", ".join(missing)
            )
        bad = archive.testzip()
        if bad:
            raise CNOIngestError(f"ZIP do CNO corrompido no membro {bad}.")
        return {name: members[name] for name in EXPECTED_FILES}


def _download(url: str, path: Path, session=requests, timeout: int = 300) -> None:
    response = session.get(url, stream=True, timeout=timeout)
    response.raise_for_status()
    with path.open("wb") as output:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                output.write(chunk)


def fetch_cno_snapshot(
    destination: Path | str = DEFAULT_DIR,
    source_url: str | None = None,
    session=requests,
) -> Path:
    """Baixa, valida e instala atomicamente o snapshot bruto do CNO."""
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    source_url = source_url or os.getenv("CNO_SOURCE_URL") or discover_source_url(session)

    with tempfile.TemporaryDirectory(
        prefix="cno-", dir=destination.parent
    ) as temporary:
        stage = Path(temporary) / "snapshot"
        stage.mkdir()
        archive_path = stage / "cno.zip"
        _download(source_url, archive_path, session=session)
        members = validate_archive(archive_path)

        with zipfile.ZipFile(archive_path) as archive:
            for canonical, member in members.items():
                with archive.open(member) as source, (stage / canonical).open("wb") as target:
                    shutil.copyfileobj(source, target)

        (stage / "source.json").write_text(
            json.dumps(
                {"dataset": DATASET_NAME, "catalog": DATASET_PAGE, "source_url": source_url},
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        installed = Path(temporary) / "installed"
        stage.replace(installed)
        if destination.exists():
            shutil.rmtree(destination)
        shutil.copytree(installed, destination)

    return destination


def main() -> None:
    parser = argparse.ArgumentParser(description="Baixa e valida o snapshot aberto do CNO.")
    parser.add_argument("--destination", type=Path, default=DEFAULT_DIR)
    parser.add_argument("--source-url", help="Sobrescreve a descoberta via dados.gov.br.")
    args = parser.parse_args()
    destination = fetch_cno_snapshot(args.destination, args.source_url)
    print(f"Snapshot CNO instalado em {destination}.")


if __name__ == "__main__":
    main()
