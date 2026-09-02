"""O acervo da Caixa não tem histórico; o nosso precisa ter.

Sobrescrever um nome fixo a cada publicação destruiria a série temporal que
este projeto existe para preservar — imóvel vendido some da lista da Caixa e
não volta. Estes testes travam o esquema de nomes que impede isso.
"""

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from archive_names import (
    BRUTO_LATEST,
    BRUTO_PREFIX,
    PARQUET_LATEST,
    PARQUET_PREFIX,
    bruto_datado,
    item_do_ano,
    parquet_datado,
)


def test_um_item_por_ano():
    assert item_do_ano(date(2026, 9, 2)) == "imoveis-caixa-economica-federal-2026"
    assert item_do_ano(date(2027, 1, 1)) == "imoveis-caixa-economica-federal-2027"


def test_o_retrato_de_cada_dia_tem_nome_proprio():
    """Duas publicações no mesmo ano não podem colidir."""
    hoje = parquet_datado(date(2026, 9, 2))
    amanha = parquet_datado(date(2026, 9, 3))

    assert hoje == "imoveis_geocoded_2026-09-02.parquet"
    assert hoje != amanha


def test_o_bruto_acompanha_a_data_do_parquet():
    quando = date(2026, 9, 2)

    assert bruto_datado(quando) == "imoveis_csv_bruto_2026-09-02.zip"
    assert parquet_datado(quando).endswith("2026-09-02.parquet")


def test_os_nomes_estaveis_nao_carregam_data():
    """imoveis_caixa.sql precisa de um alvo que não mude a cada publicação."""
    assert PARQUET_LATEST == "imoveis_geocoded.parquet"
    assert BRUTO_LATEST == "imoveis_csv_bruto.zip"
    assert not any(c.isdigit() for c in PARQUET_LATEST + BRUTO_LATEST)


def test_datado_e_estavel_compartilham_o_prefixo():
    """O gate de proveniência reconhece a fonte pelo prefixo."""
    assert parquet_datado().startswith(PARQUET_PREFIX)
    assert bruto_datado().startswith(BRUTO_PREFIX)
    assert PARQUET_LATEST.startswith(PARQUET_PREFIX)
    assert BRUTO_LATEST.startswith(BRUTO_PREFIX)
