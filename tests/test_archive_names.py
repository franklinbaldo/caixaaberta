"""O acervo da Caixa não tem histórico; o nosso precisa ter.

Sobrescrever um nome fixo a cada publicação destruiria a série temporal que
este projeto existe para preservar — imóvel vendido some da lista da Caixa e
não volta. Estes testes travam o esquema de nomes que impede isso.
"""

import sys
from datetime import UTC, date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import archive_names
from archive_names import (
    BRUTO_PREFIX,
    ITEM_PONTEIRO,
    PARQUET_PREFIX,
    bruto_datado,
    data_de_publicacao,
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
    """Não existe apelido para o mais recente entre os arquivos de dado."""
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
    quando = date(2026, 9, 2)

    assert parquet_datado(quando).startswith(PARQUET_PREFIX)
    assert bruto_datado(quando).startswith(BRUTO_PREFIX)


def test_todo_nome_exige_a_data_explicitamente():
    """Nenhuma função de nome consulta o relógio por conta própria.

    Se consultassem, uma execução atravessando a meia-noite gravaria o zip num
    dia e o Parquet no outro — e, na virada do ano, mandaria o arquivo para o
    item errado.
    """
    import inspect

    for funcao in (item_do_ano, parquet_datado, bruto_datado):
        parametro = inspect.signature(funcao).parameters["quando"]
        assert parametro.default is inspect.Parameter.empty, funcao.__name__


def test_a_data_de_publicacao_e_utc(monkeypatch):
    """Produtor e consumidor só batem se ambos falarem o mesmo fuso."""

    class RelogioFixo(datetime):
        @classmethod
        def now(cls, tz=None):
            # 23h em Brasília no dia 2 já é dia 3 em UTC.
            return datetime(2026, 9, 3, 2, 0, tzinfo=UTC).astimezone(tz)

    monkeypatch.setattr(archive_names, "datetime", RelogioFixo)
    assert data_de_publicacao() == date(2026, 9, 3)


def test_o_item_ponteiro_nao_carrega_ano():
    """O ponteiro sobrevive à virada do ano; os itens de dado, não."""
    assert ITEM_PONTEIRO == "imoveis-caixa-economica-federal"
    assert not any(c.isdigit() for c in ITEM_PONTEIRO)
