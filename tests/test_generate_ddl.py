from generate_ddl import generate_ddl


def test_generate_ddl_points_to_single_published_parquet(tmp_path):
    output_file = tmp_path / "imoveis_caixa.sql"

    generate_ddl("item-de-teste", output_file)

    sql = output_file.read_text(encoding="utf-8")
    assert "INSTALL httpfs;" in sql
    assert "LOAD httpfs;" in sql
    assert "CREATE OR REPLACE VIEW imoveis_caixa AS" in sql
    assert (
        "https://archive.org/download/item-de-teste/imoveis_geocoded.parquet" in sql
    )
    assert "imoveis_AC.parquet" not in sql
