import io
import json
import zipfile
from pathlib import Path

import pytest

from src.cno_ingest import (
    CATALOG_API,
    CNOIngestError,
    EXPECTED_FILES,
    discover_source_url,
    fetch_cno_snapshot,
    validate_archive,
)


def _zip_bytes(names=EXPECTED_FILES):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name in names:
            archive.writestr(f"CNO/{name.upper()}", f"conteudo de {name}\n")
    return buffer.getvalue()


def test_discover_source_url_follows_catalog_detail(requests_mock):
    requests_mock.get(
        CATALOG_API,
        json={
            "content": [
                {"id": "cno-id", "titulo": "Cadastro Nacional de Obras - CNO"}
            ]
        },
    )
    requests_mock.get(
        f"{CATALOG_API}/cno-id",
        json={
            "recursos": [
                {
                    "titulo": "Cadastro Nacional de Obras - CNO",
                    "formato": "CSV",
                    "url": "https://arquivos.exemplo.gov.br/cno.zip",
                }
            ]
        },
    )

    assert discover_source_url() == "https://arquivos.exemplo.gov.br/cno.zip"


def test_validate_archive_rejects_missing_table(tmp_path):
    archive = tmp_path / "cno.zip"
    archive.write_bytes(_zip_bytes(EXPECTED_FILES[:-1]))

    with pytest.raises(CNOIngestError, match="cno_totais.csv"):
        validate_archive(archive)


def test_fetch_cno_snapshot_installs_only_after_validating(tmp_path, requests_mock):
    source = "https://arquivos.exemplo.gov.br/cno.zip"
    requests_mock.get(source, content=_zip_bytes())
    destination = tmp_path / "raw"

    result = fetch_cno_snapshot(destination, source_url=source)

    assert result == destination
    assert (destination / "cno.zip").is_file()
    for name in EXPECTED_FILES:
        assert (destination / name).is_file()
    provenance = json.loads((destination / "source.json").read_text(encoding="utf-8"))
    assert provenance["source_url"] == source


def test_failed_snapshot_does_not_replace_previous_one(tmp_path, requests_mock):
    source = "https://arquivos.exemplo.gov.br/cno.zip"
    requests_mock.get(source, content=_zip_bytes(("cno.csv",)))
    destination = tmp_path / "raw"
    destination.mkdir()
    (destination / "marker.txt").write_text("anterior", encoding="utf-8")

    with pytest.raises(CNOIngestError):
        fetch_cno_snapshot(destination, source_url=source)

    assert (destination / "marker.txt").read_text(encoding="utf-8") == "anterior"
