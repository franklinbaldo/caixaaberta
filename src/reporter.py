from pathlib import Path

import pandas as pd


DEFAULT_PARQUET_PATH = Path("output_data/imoveis_geocoded.parquet")


def format_currency(value):
    if pd.isna(value):
        return "N/A"
    return f"R$ {value:,.2f}"


def generate_report(parquet_path: Path | str = DEFAULT_PARQUET_PATH):
    """Print summary statistics for the Parquet produced by the current pipeline."""
    path = Path(parquet_path)
    if not path.exists():
        raise FileNotFoundError(f"Parquet não encontrado: {path}")

    df = pd.read_parquet(path)
    if df.empty:
        raise ValueError(f"Parquet vazio: {path}")

    print(f"Real Estate Data Report: {path}")
    print("--------------------------------------------------")
    print(f"Total properties listed: {len(df)}")

    if "estado" not in df.columns:
        raise ValueError("Coluna obrigatória 'estado' ausente no Parquet")

    print("Properties per state:")
    properties_per_state = df.groupby("estado").size()
    for state, count in properties_per_state.items():
        print(f"  {state}: {count} properties")

    if "preco" in df.columns:
        prices = pd.to_numeric(df["preco"], errors="coerce")
        price_frame = df.assign(preco=prices)
        print("Average price per state:")
        for state, avg_price in price_frame.groupby("estado")["preco"].mean().items():
            print(f"  {state}: {format_currency(avg_price)}")

    if "latitude" in df.columns and "longitude" in df.columns:
        total_geocoded = df["latitude"].notna().sum()
        pct = total_geocoded / len(df) * 100
        print(
            f"Overall geocoding success rate: {pct:.1f}% "
            f"({total_geocoded} out of {len(df)} properties)"
        )


def main():
    generate_report()


if __name__ == "__main__":
    main()
