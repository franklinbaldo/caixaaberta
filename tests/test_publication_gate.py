from pathlib import Path
import sys

import pandas as pd
import pytest

import run_pipeline
from reporter import REQUIRED_PUBLICATION_COLUMNS, validate_publication_parquet


def _valid_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "link": "https://example.test/imovel/1",
                "endereco": "Rua A",
                "bairro": "CENTRO",
                "cidade": "Porto Velho",
                "estado": "RO",
                "preco": 100000.0,
            }
        ]
    )


def test_validate_publication_parquet_rejects_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError, match="Parquet não encontrado"):
        validate_publication_parquet(tmp_path / "missing.parquet")


def test_validate_publication_parquet_rejects_empty_file(tmp_path):
    path = tmp_path / "empty.parquet"
    pd.DataFrame(columns=sorted(REQUIRED_PUBLICATION_COLUMNS)).to_parquet(path)

    with pytest.raises(ValueError, match="Parquet vazio"):
        validate_publication_parquet(path)


def test_validate_publication_parquet_rejects_invalid_schema(tmp_path):
    path = tmp_path / "invalid.parquet"
    _valid_frame().drop(columns=["estado"]).to_parquet(path)

    with pytest.raises(ValueError, match="estado"):
        validate_publication_parquet(path)


def test_validate_publication_parquet_rejects_all_blank_links(tmp_path):
    path = tmp_path / "blank-links.parquet"
    frame = _valid_frame()
    frame["link"] = "  "
    frame.to_parquet(path)

    with pytest.raises(ValueError, match="sem nenhum 'link' publicável"):
        validate_publication_parquet(path)


def test_validate_publication_parquet_accepts_structurally_valid_file(tmp_path):
    path = tmp_path / "valid.parquet"
    frame = _valid_frame()
    frame.to_parquet(path)

    result = validate_publication_parquet(path)

    pd.testing.assert_frame_equal(result, frame)


def test_pipeline_does_not_upload_when_publication_gate_fails(
    monkeypatch, tmp_path
):
    called = False

    def fake_upload(**kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(run_pipeline, "DEFAULT_PARQUET_PATH", tmp_path / "missing.parquet")
    monkeypatch.setattr(run_pipeline, "upload_files_to_archive", fake_upload)
    monkeypatch.setattr(sys, "argv", ["run_pipeline.py", "--skip-processing"])

    with pytest.raises(FileNotFoundError, match="Parquet não encontrado"):
        run_pipeline.main()

    assert not called
