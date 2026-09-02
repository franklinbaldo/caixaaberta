import argparse
from datetime import date

from archive_names import ITEM_PREFIX, PARQUET_PREFIX, item_do_ano, parquet_datado

# O DDL não guarda data nem ano: monta a URL em SQL a partir de `current_date`.
# DuckDB dobra a expressão no bind, então a view aponta sozinha para o retrato
# de hoje — sem nome estável no Archive e sem regerar o arquivo na virada do
# ano. Para fixar um dia, use --data.
URL_CORRENTE = f"""'https://archive.org/download/{ITEM_PREFIX}-'
    || strftime(current_date, '%Y')
    || '/{PARQUET_PREFIX}_'
    || strftime(current_date, '%Y-%m-%d')
    || '.parquet'"""

DDL_CORRENTE = f"""INSTALL httpfs;
LOAD httpfs;

-- A view lê o retrato do dia corrente. O nome do arquivo e o item do ano são
-- calculados a partir da data: nada aqui precisa ser atualizado.
CREATE OR REPLACE VIEW imoveis_caixa AS
SELECT * FROM read_parquet(
    {URL_CORRENTE}
);
"""

DDL_FIXO = """INSTALL httpfs;
LOAD httpfs;

CREATE OR REPLACE VIEW imoveis_caixa AS
SELECT * FROM read_parquet('https://archive.org/download/{item}/{arquivo}');
"""


def generate_ddl(output_file="imoveis_caixa.sql", quando: date | None = None):
    """Gera uma view DuckDB sobre o Parquet publicado no Internet Archive.

    Sem `quando`, o DDL calcula a data em SQL e nunca precisa ser regerado.
    Com `quando`, congela um retrato específico da série.
    """
    if quando is None:
        sql_command = DDL_CORRENTE
    else:
        sql_command = DDL_FIXO.format(
            item=item_do_ano(quando), arquivo=parquet_datado(quando)
        )

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
        "--data",
        type=date.fromisoformat,
        help="Congela a view num retrato específico (AAAA-MM-DD). "
        "Sem isso, a view acompanha o dia corrente.",
    )
    parser.add_argument(
        "--output-file",
        default="imoveis_caixa.sql",
        help="O nome do arquivo de saída DDL.",
    )
    args = parser.parse_args()

    generate_ddl(args.output_file, args.data)
