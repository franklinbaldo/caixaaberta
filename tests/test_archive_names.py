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
    BRUTO_PREFIX,
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


def test_nenhum_nome_publicado_dispensa_a_data():
    """Não existe apelido para o mais recente: quem consulta calcula a data."""
    import archive_names

    publicaveis = [
        valor
        for nome, valor in vars(archive_names).items()
        if isinstance(valor, str)
        and not nome.startswith("_")
        and (valor.endswith(".parquet") or valor.endswith(".zip"))
    ]
    assert publicaveis == []


def test_datado_carrega_o_prefixo():
    """O gate de proveniência reconhece a fonte pelo prefixo."""
    assert parquet_datado().startswith(PARQUET_PREFIX)
    assert bruto_datado().startswith(BRUTO_PREFIX)
