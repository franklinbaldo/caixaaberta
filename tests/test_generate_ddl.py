from datetime import date

from archive_names import item_do_ano, parquet_datado
from generate_ddl import generate_ddl


def test_a_view_corrente_calcula_o_alvo_a_partir_da_data(tmp_path):
    """Sem nome estável no Archive, o DDL monta a URL em SQL."""
    output_file = tmp_path / "imoveis_caixa.sql"

    generate_ddl(output_file)

    sql = output_file.read_text(encoding="utf-8")
    assert "INSTALL httpfs;" in sql
    assert "LOAD httpfs;" in sql
    assert "CREATE OR REPLACE VIEW imoveis_caixa AS" in sql
    assert sql.count("current_date") == 2
    assert "imoveis-caixa-economica-federal-" in sql
    assert "imoveis_geocoded_" in sql
    # Nenhuma data literal: o arquivo não envelhece nem na virada do ano.
    assert "2026" not in sql


def test_a_view_fixa_congela_um_retrato(tmp_path):
    output_file = tmp_path / "imoveis_caixa.sql"
    quando = date(2026, 9, 2)

    generate_ddl(output_file, quando)

    sql = output_file.read_text(encoding="utf-8")
    assert (
        f"https://archive.org/download/{item_do_ano(quando)}/{parquet_datado(quando)}"
        in sql
    )
    assert "current_date" not in sql
    assert "imoveis_AC.parquet" not in sql


def test_a_view_corrente_e_executavel_no_duckdb():
    """DuckDB precisa dobrar a expressão no bind; se não dobrar, isto quebra."""
    import duckdb

    from generate_ddl import URL_CORRENTE

    url = duckdb.sql(f"SELECT {URL_CORRENTE}").fetchone()[0]

    assert url.startswith(
        "https://archive.org/download/imoveis-caixa-economica-federal-"
    )
    assert url.endswith(f"{date.today().isoformat()}.parquet")
