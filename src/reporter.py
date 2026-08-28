from pathlib import Path

import pandas as pd


DEFAULT_PARQUET_PATH = Path("output_data/imoveis_geocoded.parquet")
REQUIRED_PUBLICATION_COLUMNS = {
    "link",
    "endereco",
    "bairro",
    "cidade",
    "estado",
    "preco",
}


def validate_publication_parquet(
    parquet_path: Path | str = DEFAULT_PARQUET_PATH,
) -> pd.DataFrame:
    """Validate structural invariants required before publication."""
    path = Path(parquet_path)
    if not path.exists():
        raise FileNotFoundError(f"Parquet não encontrado: {path}")

    try:
        df = pd.read_parquet(path)
    except Exception as exc:
        raise ValueError(f"Parquet ilegível: {path}") from exc

    if df.empty:
        raise ValueError(f"Parquet vazio: {path}")

    missing = sorted(REQUIRED_PUBLICATION_COLUMNS - set(df.columns))
    if missing:
        raise ValueError(
            "Colunas obrigatórias ausentes no Parquet: " + ", ".join(missing)
        )

    links = df["link"].fillna("").astype(str).str.strip()
    if not links.ne("").any():
        raise ValueError("Parquet sem nenhum 'link' publicável")

    return df


def format_currency(value):
    if pd.isna(value):
        return "N/A"
    return f"R$ {value:,.2f}"


def generate_report(parquet_path: Path | str = DEFAULT_PARQUET_PATH):
    """Print summary statistics for the Parquet produced by the current pipeline."""
    path = Path(parquet_path)
    df = validate_publication_parquet(path)

    print(f"Real Estate Data Report: {path}")
    print("--------------------------------------------------")
    print(f"Total properties listed: {len(df)}")

    print("Properties per state:")
    properties_per_state = df.groupby("estado").size()
    for state, count in properties_per_state.items():
        print(f"  {state}: {count} properties")

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
