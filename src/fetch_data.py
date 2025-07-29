import pandas as pd
import ibis
from pathlib import Path

INPUT_DIR = "data"
OUTPUT_DIR = "output_data"

def process_local_data():
    """
    Processes local CSV files, transforms them with Ibis, and saves them as Parquet files.
    """
    input_path = Path(INPUT_DIR)
    output_path = Path(OUTPUT_DIR)
    output_path.mkdir(parents=True, exist_ok=True)

    csv_files = list(input_path.glob("imoveis_*.csv"))

    if not csv_files:
        print(f"Nenhum arquivo CSV encontrado em {INPUT_DIR}")
        return

    # Usando um banco de dados em memória para o processamento com Ibis
    conn = ibis.duckdb.connect()

    for csv_file in csv_files:
        state = csv_file.stem.split("_")[1]
        print(f"Processando arquivo: {csv_file.name}")

        try:
            # Carregar o CSV para uma tabela no DuckDB
            conn.create_table(f"imoveis_{state}", pd.read_csv(csv_file), overwrite=True)

            # Transformar com Ibis
            imoveis_table = conn.table(f"imoveis_{state}")
            imoveis_table = imoveis_table.mutate(bairro=imoveis_table.bairro.fill_null("").upper().strip())
            imoveis_table = imoveis_table.drop_null('link')
            imoveis_table = imoveis_table.distinct()

            # Salvar como Parquet
            output_file = output_path / f"imoveis_{state}.parquet"
            df = imoveis_table.to_pandas()
            df.to_parquet(output_file, index=False)
            print(f"Salvo dados processados para {output_file}")

        except Exception as e:
            print(f"Erro ao processar o arquivo {csv_file.name}: {e}")

if __name__ == "__main__":
    process_local_data()
