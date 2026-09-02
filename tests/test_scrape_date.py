from datetime import date

import pandas as pd
import pytest

import fetch_data
from archive_names import parquet_datado
from reporter import validate_publication_parquet


QUANDO = date(2026, 9, 2)


def test_processamento_grava_scrape_date_em_todas_as_linhas(tmp_path, monkeypatch):
    entrada = tmp_path / "data"
    saida = tmp_path / "output"
    entrada.mkdir()
    pd.DataFrame(
        {
            "link": ["1", "2"],
            "endereco": ["Rua A", "Rua B"],
            "bairro": ["Centro", "Centro"],
            "cidade": ["Porto Velho", "Cacoal"],
            "estado": ["RO", "RO"],
            "preco": [100000.0, 200000.0],
            "latitude": [-8.76, -11.43],
            "longitude": [-63.90, -61.44],
        }
    ).to_csv(entrada / "imoveis_RO.csv", index=False)

    monkeypatch.setattr(fetch_data, "INPUT_DIR", str(entrada))
    monkeypatch.setattr(fetch_data, "OUTPUT_DIR", str(saida))

    fetch_data.process_local_data(quando=QUANDO)

    path = saida / parquet_datado(QUANDO)
    df = pd.read_parquet(path)
    assert set(pd.to_datetime(df["scrape_date"]).dt.date) == {QUANDO}
    validate_publication_parquet(path)


def test_gate_recusa_data_interna_diferente_do_nome(tmp_path):
    path = tmp_path / parquet_datado(QUANDO)
    pd.DataFrame(
        {
            "link": ["1"],
            "endereco": ["Rua A"],
            "bairro": ["CENTRO"],
            "cidade": ["Porto Velho"],
            "estado": ["RO"],
            "preco": [100000.0],
            "scrape_date": [date(2026, 9, 1)],
        }
    ).to_parquet(path, index=False)

    with pytest.raises(ValueError, match="não corresponde a scrape_date"):
        validate_publication_parquet(path)


def test_gate_recusa_duas_datas_no_mesmo_snapshot(tmp_path):
    path = tmp_path / parquet_datado(QUANDO)
    pd.DataFrame(
        {
            "link": ["1", "2"],
            "endereco": ["Rua A", "Rua B"],
            "bairro": ["CENTRO", "CENTRO"],
            "cidade": ["Porto Velho", "Cacoal"],
            "estado": ["RO", "RO"],
            "preco": [100000.0, 200000.0],
            "scrape_date": [QUANDO, date(2026, 9, 1)],
        }
    ).to_parquet(path, index=False)

    with pytest.raises(ValueError, match="scrape_date deve ser único"):
        validate_publication_parquet(path)
