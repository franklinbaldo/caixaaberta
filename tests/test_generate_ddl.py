import json
from datetime import date

import duckdb
import pytest

from archive_names import item_do_ano, parquet_datado, url_do_manifesto
from generate_ddl import generate_ddl


def test_a_view_corrente_segue_o_manifesto(tmp_path):
    """O calendário não sabe se a publicação do dia aconteceu; o manifesto sabe."""
    output_file = tmp_path / "imoveis_caixa.sql"

    generate_ddl(output_file)

    sql = output_file.read_text(encoding="utf-8")
    assert "CREATE OR REPLACE VIEW imoveis_caixa AS" in sql
    assert url_do_manifesto() in sql
    assert "getvariable('imoveis_caixa_snapshot')" in sql
    # Nem data, nem ano, nem relógio: o arquivo não envelhece.
    assert "current_date" not in sql
    assert "2026" not in sql


def test_a_view_fixa_congela_um_retrato(tmp_path):
    output_file = tmp_path / "imoveis_caixa.sql"
    quando = date(2026, 9, 2)

    generate_ddl(output_file, quando)

    sql = output_file.read_text(encoding="utf-8")
    assert item_do_ano(quando) in sql
    assert parquet_datado(quando) in sql
    assert "getvariable" not in sql


@pytest.fixture
def manifesto_local(tmp_path):
    """Um manifesto e um Parquet em disco, no lugar do Archive."""
    parquet = tmp_path / "imoveis_geocoded_2026-09-02.parquet"
    duckdb.sql("SELECT 'RO' AS estado, 1 AS n").to_parquet(str(parquet))

    manifesto = tmp_path / "latest.json"
    manifesto.write_text(
        json.dumps({"data": "2026-09-02", "parquet_url": str(parquet)}),
        encoding="utf-8",
    )
    return manifesto, parquet


@pytest.mark.parametrize("fuso", ["UTC", "America/Sao_Paulo", "Asia/Tokyo"])
def test_a_view_resolve_igual_em_qualquer_fuso(tmp_path, manifesto_local, fuso):
    """O fuso da sessão do consumidor não pode escolher outro retrato.

    Era o furo de derivar o alvo de `current_date`: em UTC-3 ou UTC+9 o
    consumidor calcularia outro dia ao redor da meia-noite, e outro item
    inteiro na virada do ano.
    """
    manifesto, parquet = manifesto_local
    output_file = tmp_path / "imoveis_caixa.sql"
    generate_ddl(output_file)

    sql = output_file.read_text(encoding="utf-8").replace(
        url_do_manifesto(), str(manifesto)
    )

    con = duckdb.connect()
    con.execute(f"SET TimeZone = '{fuso}'")
    for comando in filter(str.strip, sql.split(";")):
        con.execute(comando)

    assert con.sql("SELECT estado FROM imoveis_caixa").fetchone()[0] == "RO"
    assert con.sql("SELECT getvariable('imoveis_caixa_snapshot')").fetchone()[0] == str(
        parquet
    )
