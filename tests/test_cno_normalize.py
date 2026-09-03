from pathlib import Path

import pandas as pd
import pytest

from src.cno_normalize import CNOTransformError, normalize_snapshot


def _write(path: Path, header: str, rows: list[str]) -> None:
    path.write_text(header + "\n" + "\n".join(rows) + "\n", encoding="utf-8")


def _fixture(raw: Path) -> None:
    raw.mkdir()
    _write(
        raw / "cno.csv",
        "CNO;Nome;Data de início;Data de registro;CEP;Nome do município;Tipo de logradouro;Logradouro;Número do logradouro;Bairro;Estado;Complemento;Área total;Situação",
        [
            "123;OBRA A;2020-01-02;2020-01-03;76801000;Porto Velho;Rua;Dom Pedro II;123;Centro;RO;AP 2;120,5;02 - ATIVA",
            "456;OBRA B;2021-04-05;2021-04-06;01001000;São Paulo;Praça;da Sé;S/N;Sé;SP;;80;15 - ENCERRADA",
        ],
    )
    _write(
        raw / "cno_areas.csv",
        "CNO;Categoria;Destinação;Tipo de Obra;Tipo de Área;Tipo de Área Complementar;Metragem",
        [
            "123;0 - Obra Nova;0 - Residencial unifamiliar;0 - Alvenaria;P;;100,5",
            "123;0 - Obra Nova;0 - Residencial unifamiliar;0 - Alvenaria;C;2 - Piscina;20",
            "456;4 - Existente;2 - Comercial salas e lojas;0 - Alvenaria;P;;80",
        ],
    )
    _write(
        raw / "cno_cnaes.csv",
        "CNO;CNAE;Data de registro",
        ["123;4120400;2020-01-03", "456;4399199;2021-04-06"],
    )
    _write(
        raw / "cno_vinculos.csv",
        "CNO;Data de início;Data de fim;Qualificação do contribuinte;NI do responsável",
        ["123;2020-01-02;;0053;12345678000199", "456;2021-04-05;2023-09-01;0070;"],
    )
    _write(
        raw / "cno_totais.csv",
        "Total de obras;Total de cnaes;Total de áreas;Total de vínculos",
        ["2;2;3;2"],
    )


def test_normalize_snapshot_preserves_one_to_many_areas(tmp_path):
    raw = tmp_path / "raw"
    output = tmp_path / "normalized"
    _fixture(raw)

    paths = normalize_snapshot(raw, output)

    cno = pd.read_parquet(paths["cno.csv"])
    areas = pd.read_parquet(paths["cno_areas.csv"])
    assert len(cno) == 2
    assert len(areas) == 3
    assert list(areas[areas["cno"] == "000000000123"]["metragem_num"]) == [100.5, 20.0]


def test_normalize_snapshot_adds_address_keys_and_typed_fields(tmp_path):
    raw = tmp_path / "raw"
    output = tmp_path / "normalized"
    _fixture(raw)

    paths = normalize_snapshot(raw, output)
    cno = pd.read_parquet(paths["cno.csv"])
    first = cno.iloc[0]

    assert first["cno"] == "000000000123"
    assert first["municipio_normalizado"] == "PORTO VELHO"
    assert first["logradouro_normalizado"] == "DOM PEDRO II"
    assert first["numero_normalizado"] == "123"
    assert first["cep_normalizado"] == "76801000"
    assert first["situacao_codigo"] == "02"
    assert first["situacao_descricao"] == "ATIVA"
    assert first["area_total_num"] == 120.5
    assert str(first["data_de_inicio_iso"]) == "2020-01-02"


def test_normalize_snapshot_checks_control_totals(tmp_path):
    raw = tmp_path / "raw"
    output = tmp_path / "normalized"
    _fixture(raw)
    (raw / "cno_totais.csv").write_text(
        "Total de obras;Total de cnaes;Total de áreas;Total de vínculos\n99;2;3;2\n",
        encoding="utf-8",
    )

    with pytest.raises(CNOTransformError, match="declara 99"):
        normalize_snapshot(raw, output)
