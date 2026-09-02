from pathlib import Path

import pandas as pd

from archive_names import parquet_datado

OUTPUT_DIR = Path("output_data")


def parquet_do_dia(quando) -> Path:
    """Caminho local do retrato de um dia. Não existe nome sem data."""
    return OUTPUT_DIR / parquet_datado(quando)


REQUIRED_PUBLICATION_COLUMNS = {
    "link",
    "endereco",
    "bairro",
    "cidade",
    "estado",
    "preco",
}

# Espelha os conceitos Modalidade de knowledge/. A divergência entre as duas
# listas é recusada por scripts/check_bundle_contract.py.
KNOWN_MODALIDADES = frozenset(
    {
        "Leilão SFI - Edital Único",
        "Licitação Aberta",
        "Venda Direta Online",
        "Venda Online",
    }
)


def undocumented_modalidades(df: pd.DataFrame) -> list[str]:
    """Modalidades presentes no Parquet que o bundle não descreve."""
    if "modalidade" not in df.columns:
        return []
    observed = set(df["modalidade"].dropna().astype(str).str.strip())
    return sorted(observed - KNOWN_MODALIDADES - {""})


def validate_publication_parquet(
    parquet_path: Path | str,
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


def generate_report(parquet_path: Path | str):
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

    if "modalidade" in df.columns:
        print("Properties per modalidade:")
        for modalidade, count in df.groupby("modalidade").size().items():
            print(f"  {modalidade}: {count} properties")

        novas = undocumented_modalidades(df)
        if novas:
            print(
                "ATENÇÃO: modalidade sem conceito em knowledge/: "
                + ", ".join(novas)
                + ". Estatísticas de preço que a incluam misturam regimes de "
                "venda diferentes."
            )

    if "latitude" in df.columns and "longitude" in df.columns:
        total_geocoded = df["latitude"].notna().sum()
        pct = total_geocoded / len(df) * 100
        print(
            f"Overall geocoding success rate: {pct:.1f}% "
            f"({total_geocoded} out of {len(df)} properties)"
        )

    # A taxa acima conta como sucesso o centroide de município. Sem a quebra
    # por precisão ela sugere um dataset localizado que não existe.
    if "precisao" in df.columns:
        print("Geocoding precision:")
        for nivel, count in df["precisao"].value_counts().items():
            print(f"  {nivel}: {count} ({count / len(df) * 100:.1f}%)")
        rua = df["precisao"].isin(["logradouro_localidade", "logradouro"]).sum()
        print(f"  ao nível de logradouro ou melhor: {rua} ({rua / len(df) * 100:.1f}%)")


def main():
    generate_report()


if __name__ == "__main__":
    main()
