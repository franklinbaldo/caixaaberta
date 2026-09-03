import sys
from datetime import date
from pathlib import Path

import run_pipeline
from archive_names import cno_matches_datado


QUANDO = date(2026, 9, 2)


def test_workflow_de_publicacao_ativa_cno():
    workflow = Path(".github/workflows/main.yml").read_text(encoding="utf-8")
    assert "src/run_pipeline.py --with-cno" in workflow


def test_with_cno_prepara_indice_antes_do_processamento(monkeypatch, tmp_path):
    calls = []
    raw = tmp_path / "raw"
    normalized = Path("cno_data/normalized")

    monkeypatch.setattr(run_pipeline, "fetch_cno_snapshot", lambda: calls.append("fetch-cno") or raw)
    monkeypatch.setattr(
        run_pipeline,
        "normalize_snapshot",
        lambda source, target: calls.append(("normalize-cno", source, target)),
    )
    monkeypatch.setattr(
        run_pipeline,
        "process_local_data",
        lambda **kwargs: calls.append(("process", kwargs["cno_dir"])),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_pipeline.py", "--skip-fetch", "--skip-upload", "--with-cno"],
    )

    run_pipeline.main()

    assert calls == [
        "fetch-cno",
        ("normalize-cno", raw, normalized),
        ("process", normalized),
    ]


def test_publicacao_com_cno_leva_tabela_de_evidencias(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    output = tmp_path / "output_data"
    output.mkdir()
    evidence = output / cno_matches_datado(QUANDO)

    monkeypatch.setattr(run_pipeline, "fetch_cno_snapshot", lambda: tmp_path / "raw")
    monkeypatch.setattr(run_pipeline, "normalize_snapshot", lambda *_: None)

    def process(**kwargs):
        assert kwargs["cno_dir"] == Path("cno_data/normalized")
        evidence.write_bytes(b"PAR1")

    monkeypatch.setattr(run_pipeline, "process_local_data", process)
    monkeypatch.setattr(run_pipeline, "validate_publication_parquet", lambda *_: None)
    monkeypatch.setattr(run_pipeline, "parquet_do_dia", lambda *_: output / "principal.parquet")
    monkeypatch.setattr(run_pipeline, "artefatos_da_publicacao", lambda *_args, **_kw: ["principal.parquet"])
    monkeypatch.setattr(run_pipeline, "publicar_manifesto", lambda *_args, **_kw: None)
    uploaded = []
    monkeypatch.setattr(
        run_pipeline,
        "upload_files_to_archive",
        lambda **kwargs: uploaded.extend(kwargs["files"]),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_pipeline.py",
            "--skip-fetch",
            "--with-cno",
            "--upload-dry-run",
            "--data",
            QUANDO.isoformat(),
        ],
    )

    run_pipeline.main()

    assert str(Path("output_data") / cno_matches_datado(QUANDO)) in uploaded
