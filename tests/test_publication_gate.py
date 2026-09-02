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


def test_pipeline_does_not_upload_when_publication_gate_fails(monkeypatch, tmp_path):
    called = False

    def fake_upload(**kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(
        run_pipeline, "parquet_do_dia", lambda: tmp_path / "missing.parquet"
    )
    monkeypatch.setattr(run_pipeline, "upload_files_to_archive", fake_upload)
    monkeypatch.setattr(sys, "argv", ["run_pipeline.py", "--skip-processing"])

    with pytest.raises(FileNotFoundError, match="Parquet não encontrado"):
        run_pipeline.main()

    assert not called


def test_pipeline_fetches_before_processing(monkeypatch, tmp_path):
    calls = []

    monkeypatch.setattr(
        run_pipeline, "fetch_all_states", lambda **kw: calls.append("fetch")
    )
    monkeypatch.setattr(
        run_pipeline, "process_local_data", lambda: calls.append("process")
    )
    monkeypatch.setattr(sys, "argv", ["run_pipeline.py", "--skip-upload"])

    run_pipeline.main()

    assert calls == ["fetch", "process"]


def test_pipeline_skip_fetch_does_not_download(monkeypatch, tmp_path):
    def fail_fetch(**kwargs):
        raise AssertionError("fetch não deveria ser chamado com --skip-fetch")

    monkeypatch.setattr(run_pipeline, "fetch_all_states", fail_fetch)
    monkeypatch.setattr(run_pipeline, "process_local_data", lambda: None)
    monkeypatch.setattr(
        sys, "argv", ["run_pipeline.py", "--skip-fetch", "--skip-upload"]
    )

    run_pipeline.main()


def test_undocumented_modalidades_flags_only_the_unknown():
    from reporter import undocumented_modalidades

    frame = pd.DataFrame(
        {
            "modalidade": [
                "Venda Direta Online",
                "Leilão SFI - Edital Único",
                "Leilão Presencial",
                None,
                "  ",
            ]
        }
    )

    assert undocumented_modalidades(frame) == ["Leilão Presencial"]


def test_undocumented_modalidades_without_the_column():
    from reporter import undocumented_modalidades

    assert undocumented_modalidades(pd.DataFrame({"link": ["1"]})) == []


def test_skip_processing_alone_does_not_download(monkeypatch, tmp_path):
    """--skip-processing publica o Parquet existente sem depender da Caixa."""

    def fail_fetch(**kwargs):
        raise AssertionError("fetch não deveria ser chamado com --skip-processing")

    uploaded = []

    monkeypatch.setattr(run_pipeline, "fetch_all_states", fail_fetch)
    monkeypatch.setattr(
        run_pipeline, "process_local_data", lambda: pytest.fail("não processa")
    )
    monkeypatch.setattr(run_pipeline, "validate_publication_parquet", lambda path: None)
    monkeypatch.setattr(
        run_pipeline, "upload_files_to_archive", lambda **kw: uploaded.append(kw)
    )
    monkeypatch.setattr(
        sys, "argv", ["run_pipeline.py", "--skip-processing", "--upload-dry-run"]
    )

    run_pipeline.main()

    assert len(uploaded) == 1


def test_skip_processing_republica_sem_exigir_o_bruto(monkeypatch, tmp_path):
    """A única publicação sem a fonte é declarada, não implícita."""
    chamadas = []

    monkeypatch.setattr(
        run_pipeline, "fetch_all_states", lambda **kw: pytest.fail("não baixa")
    )
    monkeypatch.setattr(
        run_pipeline, "process_local_data", lambda: pytest.fail("não processa")
    )
    monkeypatch.setattr(run_pipeline, "validate_publication_parquet", lambda path: None)
    monkeypatch.setattr(
        run_pipeline, "upload_files_to_archive", lambda **kw: chamadas.append(kw)
    )
    monkeypatch.setattr(
        sys, "argv", ["run_pipeline.py", "--skip-processing", "--upload-dry-run"]
    )

    run_pipeline.main()

    assert chamadas[0]["exigir_bruto"] is False


def test_publicacao_normal_exige_o_bruto(monkeypatch, tmp_path):
    chamadas = []

    monkeypatch.setattr(run_pipeline, "fetch_all_states", lambda **kw: None)
    monkeypatch.setattr(run_pipeline, "process_local_data", lambda: None)
    monkeypatch.setattr(run_pipeline, "validate_publication_parquet", lambda path: None)
    monkeypatch.setattr(
        run_pipeline, "upload_files_to_archive", lambda **kw: chamadas.append(kw)
    )
    monkeypatch.setattr(
        sys, "argv", ["run_pipeline.py", "--skip-fetch", "--upload-dry-run"]
    )

    run_pipeline.main()

    assert chamadas[0]["exigir_bruto"] is True
