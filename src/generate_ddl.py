import argparse

from archive_names import PARQUET_LATEST, item_do_ano


def generate_ddl(identifier, output_file="imoveis_caixa.sql"):
    """Gera uma view DuckDB sobre o Parquet publicado no Internet Archive.

    Aponta para o nome estável do item, que é sempre a publicação mais recente
    daquele ano. Para consultar um retrato específico, troque o nome do arquivo
    pela versão datada — `imoveis_geocoded_2026-09-02.parquet` e afins, listados
    em https://archive.org/metadata/<identificador>.

    O identificador traz o ano, então o DDL versionado precisa ser regerado na
    virada: `python src/generate_ddl.py`.
    """
    base_url = f"https://archive.org/download/{identifier}"
    parquet_url = f"{base_url}/{PARQUET_LATEST}"

    sql_command = f"""INSTALL httpfs;
LOAD httpfs;

CREATE OR REPLACE VIEW imoveis_caixa AS
SELECT * FROM read_parquet('{parquet_url}');
"""

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(sql_command)

    print(f"Arquivo DDL '{output_file}' gerado com sucesso.")
    print("Para usar no DuckDB CLI, execute:")
    print(f".read {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Gera um arquivo DDL para acessar dados de imóveis do Internet Archive com DuckDB."
    )
    parser.add_argument(
        "--identifier",
        default=item_do_ano(),
        help="O identificador do item no Internet Archive.",
    )
    parser.add_argument(
        "--output-file",
        default="imoveis_caixa.sql",
        help="O nome do arquivo de saída DDL.",
    )
    args = parser.parse_args()

    generate_ddl(args.identifier, args.output_file)
