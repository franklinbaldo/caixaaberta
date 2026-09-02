from datetime import date

import pandas as pd
import pytest

from compare_snapshots import RESULT_COLUMNS, compare_snapshots


D1 = date(2026, 9, 1)
D2 = date(2026, 9, 2)


def _row(link, scrape_date, **changes):
    row = {
        "link": link,
        "scrape_date": scrape_date,
        "estado": "RO",
        "cidade": "Porto Velho",
        "endereco": f"Rua {link}",
        "preco": 100000.0,
        "avaliacao": 120000.0,
        "desconto": 10.0,
        "modalidade": "Venda Direta Online",
    }
    row.update(changes)
    return row


def _write(path, rows):
    pd.DataFrame(rows).to_parquet(path, index=False)


def test_deriva_entrada_saida_e_alteracao(tmp_path):
    anterior = tmp_path / "anterior.parquet"
    atual = tmp_path / "atual.parquet"
    _write(
        anterior,
        [
            _row("fica", D1),
            _row("sai", D1),
            _row("muda", D1, preco=100000.0, desconto=10.0),
        ],
    )
    _write(
        atual,
        [
            _row("fica", D2),
            _row("entra", D2),
            _row("muda", D2, preco=90000.0, desconto=25.0),
        ],
    )

    result = compare_snapshots(anterior, atual)
    by_link = result.set_index("link")

    assert set(by_link.index) == {"entra", "sai", "muda"}
    assert by_link.loc["entra", "mudanca"] == "entrou_no_estoque"
    assert by_link.loc["sai", "mudanca"] == "saiu_do_estoque"
    assert by_link.loc["muda", "mudanca"] == "alterou"
    assert set(by_link.loc["muda", "campos_alterados"].split(",")) == {
        "preco",
        "desconto",
    }
    assert by_link.loc["muda", "preco_anterior"] == 100000.0
    assert by_link.loc["muda", "preco_atual"] == 90000.0


def test_imovel_igual_nao_vira_evento(tmp_path):
    anterior = tmp_path / "anterior.parquet"
    atual = tmp_path / "atual.parquet"
    _write(anterior, [_row("igual", D1)])
    _write(atual, [_row("igual", D2)])

    result = compare_snapshots(anterior, atual)

    assert result.empty
    assert list(result.columns) == list(RESULT_COLUMNS)


def test_nao_inferimos_venda_quando_o_imovel_some(tmp_path):
    anterior = tmp_path / "anterior.parquet"
    atual = tmp_path / "atual.parquet"
    _write(anterior, [_row("sumiu", D1), _row("fica", D1)])
    _write(atual, [_row("fica", D2)])

    result = compare_snapshots(anterior, atual)

    assert result.iloc[0]["mudanca"] == "saiu_do_estoque"
    assert "vend" not in result.iloc[0]["mudanca"]


def test_link_duplicado_e_recusado(tmp_path):
    anterior = tmp_path / "anterior.parquet"
    atual = tmp_path / "atual.parquet"
    _write(anterior, [_row("dup", D1), _row("dup", D1, preco=90000.0)])
    _write(atual, [_row("dup", D2)])

    with pytest.raises(ValueError, match="link precisa ser único"):
        compare_snapshots(anterior, atual)


def test_ordem_das_datas_e_recusada(tmp_path):
    anterior = tmp_path / "anterior.parquet"
    atual = tmp_path / "atual.parquet"
    _write(anterior, [_row("1", D2)])
    _write(atual, [_row("1", D1)])

    with pytest.raises(ValueError, match="deve ser posterior"):
        compare_snapshots(anterior, atual)


def test_pode_gravar_o_derivado_em_parquet(tmp_path):
    anterior = tmp_path / "anterior.parquet"
    atual = tmp_path / "atual.parquet"
    output = tmp_path / "mudancas.parquet"
    _write(anterior, [_row("sai", D1), _row("fica", D1)])
    _write(atual, [_row("fica", D2), _row("entra", D2)])

    compare_snapshots(anterior, atual, output)

    gravado = pd.read_parquet(output)
    assert set(gravado["mudanca"]) == {"entrou_no_estoque", "saiu_do_estoque"}


def test_pode_gravar_derivado_vazio_com_schema(tmp_path):
    anterior = tmp_path / "anterior.parquet"
    atual = tmp_path / "atual.parquet"
    output = tmp_path / "sem-mudancas.parquet"
    _write(anterior, [_row("igual", D1)])
    _write(atual, [_row("igual", D2)])

    compare_snapshots(anterior, atual, output)

    gravado = pd.read_parquet(output)
    assert gravado.empty
    assert list(gravado.columns) == list(RESULT_COLUMNS)
