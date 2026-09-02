import argparse
from datetime import date

from archive_names import item_do_ano, parquet_datado, url_do_manifesto, url_no_item

# O calendário não serve como ponteiro para o último retrato. Duas razões:
# a publicação do dia pode falhar, e aí "hoje" aponta para um arquivo que não
# existe; e `current_date` no DuckDB é o dia no fuso da sessão, que é o do
# sistema do consumidor — em UTC-3 ou UTC+9 ele calcula outro dia por horas ao
# redor da meia-noite, e outro item inteiro na virada do ano.
#
# Por isso a view lê um manifesto: um JSON minúsculo, o único nome sobrescrito
# a cada publicação, que guarda o endereço do retrato que de fato subiu.
# `SET VARIABLE` existe porque read_parquet não aceita subconsulta.
DDL_CORRENTE = f"""INSTALL httpfs;
LOAD httpfs;
INSTALL json;
LOAD json;

-- O manifesto aponta para o último retrato efetivamente publicado. Nada aqui
-- depende do relógio nem do fuso de quem consulta, e nada precisa ser
-- regerado — nem na virada do ano.
SET VARIABLE imoveis_caixa_snapshot = (
    SELECT parquet_url FROM read_json_auto('{url_do_manifesto()}')
);

CREATE OR REPLACE VIEW imoveis_caixa AS
SELECT * FROM read_parquet(getvariable('imoveis_caixa_snapshot'));
"""

DDL_FIXO = """INSTALL httpfs;
LOAD httpfs;

-- Retrato de {data}, congelado.
CREATE OR REPLACE VIEW imoveis_caixa AS
SELECT * FROM read_parquet('{url}');
"""


def generate_ddl(output_file="imoveis_caixa.sql", quando: date | None = None):
    """Gera uma view DuckDB sobre o Parquet publicado no Internet Archive.

    Sem `quando`, a view segue o manifesto e nunca precisa ser regerada.
    Com `quando`, congela um retrato específico da série.
    """
    if quando is None:
        sql_command = DDL_CORRENTE
    else:
        sql_command = DDL_FIXO.format(
            data=quando.isoformat(),
            url=url_no_item(item_do_ano(quando), parquet_datado(quando)),
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
        "Sem isso, a view segue o manifesto do último retrato publicado.",
    )
    parser.add_argument(
        "--output-file",
        default="imoveis_caixa.sql",
        help="O nome do arquivo de saída DDL.",
    )
    args = parser.parse_args()

    generate_ddl(args.output_file, args.data)
