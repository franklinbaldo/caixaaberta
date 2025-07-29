import argparse
from pathlib import Path

def generate_ddl(identifier, output_file="imoveis_caixa.sql"):
    """
    Generates a DDL script to create a view in DuckDB that reads Parquet files from the Internet Archive.

    Args:
        identifier (str): The Internet Archive item identifier.
        output_file (str): The name of the output DDL file.
    """
    base_url = f"https://archive.org/download/{identifier}"

    # Lista de estados para gerar as URLs dos arquivos Parquet
    states = [
        "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS",
        "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC",
        "SE", "SP", "TO",
    ]

    parquet_files = [f"{base_url}/imoveis_{state}.parquet" for state in states]

    # Cria o comando SQL para criar a view
    sql_command = f"""
CREATE OR REPLACE VIEW imoveis_caixa AS
SELECT * FROM read_parquet({parquet_files});
"""

    # Salva o comando em um arquivo .sql
    with open(output_file, "w") as f:
        f.write(sql_command)

    print(f"Arquivo DDL '{output_file}' gerado com sucesso.")
    print("Para usar, execute o seguinte comando no DuckDB:")
    print(f"INSTALL httpfs;")
    print(f"LOAD httpfs;")
    print(f"IMPORT DATABASE FROM '{output_file}';")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gera um arquivo DDL para acessar dados de imóveis do Internet Archive com DuckDB.")
    parser.add_argument("--identifier", default="imoveis-caixa-economica-federal", help="O identificador do item no Internet Archive.")
    parser.add_argument("--output-file", default="imoveis_caixa.sql", help="O nome do arquivo de saída DDL.")
    args = parser.parse_args()

    generate_ddl(args.identifier, args.output_file)
