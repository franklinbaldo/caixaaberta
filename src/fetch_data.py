import pandas as pd
import ibis
from pathlib import Path
import os
from geocoding_utils import get_coordinates_for_address

INPUT_DIR = "data"
OUTPUT_DIR = "output_data"

def process_local_data():
    """
    Processes local CSV files, unnions them, transforms with Ibis,
    geocodes, and saves as a single Parquet file.
    """
    input_path = Path(INPUT_DIR)
    output_path = Path(OUTPUT_DIR)
    output_path.mkdir(parents=True, exist_ok=True)

    csv_files = list(input_path.glob("imoveis_*.csv"))

    if not csv_files:
        print(f"Nenhum arquivo CSV encontrado em {INPUT_DIR}")
        return

    conn = ibis.duckdb.connect()

    # Load all CSVs into a single table
    all_tables = []
    for csv_file in csv_files:
        table_name = f"imoveis_{csv_file.stem.split('_')[1]}"
        df = pd.read_csv(csv_file)
        # Ensure 'foto' and 'bairro' columns are string type
        if 'foto' in df.columns:
            df['foto'] = df['foto'].astype(str)
        if 'bairro' in df.columns:
            df['bairro'] = df['bairro'].astype(str)
        conn.create_table(table_name, df, overwrite=True)
        all_tables.append(conn.table(table_name))

    # Union all tables
    imoveis_table = ibis.union(*all_tables)

    # Basic transformations
    imoveis_table = imoveis_table.mutate(bairro=imoveis_table.bairro.fill_null("").upper().strip())
    imoveis_table = imoveis_table.drop_null('link')
    imoveis_table = imoveis_table.distinct()

    # Geocoding
    df = imoveis_table.to_pandas()

    # Ensure latitude and longitude columns exist
    if 'latitude' not in df.columns:
        df['latitude'] = pd.NA
    if 'longitude' not in df.columns:
        df['longitude'] = pd.NA

    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

    rows_to_geocode = df['latitude'].isnull()

    if rows_to_geocode.any():
        address_cols = ['endereco', 'bairro', 'cidade', 'estado']
        df['full_address'] = df[rows_to_geocode][address_cols].fillna('').astype(str).agg(', '.join, axis=1)

        api_key = os.getenv("GEOCODER_KEY")
        if not api_key:
            print("Warning: GEOCODER_KEY environment variable not set. Geocoding may fail or be rate-limited.")

        print(f"Starting geocoding for {rows_to_geocode.sum()} rows...")
        geocoded_coords = df.loc[rows_to_geocode, 'full_address'].apply(
            lambda addr: get_coordinates_for_address(addr, api_key=api_key) if pd.notna(addr) and addr.strip() else (None, None)
        )

        if not geocoded_coords.empty:
            coords_df = pd.DataFrame(geocoded_coords.tolist(), index=df[rows_to_geocode].index, columns=['latitude_new', 'longitude_new'])
            df.loc[rows_to_geocode, 'latitude'] = coords_df['latitude_new']
            df.loc[rows_to_geocode, 'longitude'] = coords_df['longitude_new']

        df = df.drop(columns=['full_address'])

    # Save as a single Parquet file
    output_file = output_path / "imoveis_geocoded.parquet"
    df.to_parquet(output_file, index=False)
    print(f"Salvo dados processados para {output_file}")

if __name__ == "__main__":
    process_local_data()
