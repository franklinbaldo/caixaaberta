"""Deriva mudanças entre dois snapshots imutáveis do Caixa Aberta.

A camada é deliberadamente stateless: recebe dois Parquets e produz outro.
Ausência no snapshot seguinte significa apenas `saiu_do_estoque`; não inferimos
venda, arrematação ou qualquer causa que a fonte não prove.
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import pandas as pd

TRACKED_FIELDS = ("preco", "avaliacao", "desconto", "modalidade")
CONTEXT_FIELDS = ("estado", "cidade", "endereco")


def _snapshot_date(df: pd.DataFrame, origem: str) -> date:
    if "scrape_date" not in df.columns:
        raise ValueError(f"{origem}: coluna scrape_date ausente")
    datas = pd.to_datetime(df["scrape_date"], errors="coerce").dt.date.dropna().unique()
    if len(datas) != 1:
        raise ValueError(
            f"{origem}: scrape_date deve ser único e válido; encontrados {list(datas)}"
        )
    return datas[0]


def _load_snapshot(path: str | Path) -> tuple[pd.DataFrame, date]:
    origem = str(path)
    df = pd.read_parquet(path)
    if df.empty:
        raise ValueError(f"{origem}: snapshot vazio")

    obrigatorias = {"link", "scrape_date", *TRACKED_FIELDS, *CONTEXT_FIELDS}
    faltantes = sorted(obrigatorias - set(df.columns))
    if faltantes:
        raise ValueError(f"{origem}: colunas ausentes: {', '.join(faltantes)}")

    links = df["link"].fillna("").astype(str).str.strip()
    if links.eq("").any():
        raise ValueError(f"{origem}: link vazio impede reconciliar snapshots")
    if links.duplicated().any():
        duplicados = sorted(links[links.duplicated(keep=False)].unique())
        raise ValueError(
            f"{origem}: link precisa ser único para comparar snapshots; "
            f"duplicados: {duplicados[:5]}"
        )

    out = df.copy()
    out["link"] = links
    return out.set_index("link", drop=False), _snapshot_date(out, origem)


def _same(a, b) -> bool:
    a_null = bool(pd.isna(a))
    b_null = bool(pd.isna(b))
    if a_null or b_null:
        return a_null and b_null
    return bool(a == b)


def _context(old, new, field):
    if new is not None and not pd.isna(new[field]):
        return new[field]
    if old is not None:
        return old[field]
    return None


def compare_snapshots(
    anterior: str | Path,
    atual: str | Path,
    output: str | Path | None = None,
) -> pd.DataFrame:
    """Compara dois snapshots pelo `link` e devolve somente mudanças."""
    old, old_date = _load_snapshot(anterior)
    new, new_date = _load_snapshot(atual)
    if new_date <= old_date:
        raise ValueError(
            f"snapshot atual deve ser posterior ao anterior: {new_date} <= {old_date}"
        )

    rows = []
    for link in sorted(set(old.index) | set(new.index)):
        old_row = old.loc[link] if link in old.index else None
        new_row = new.loc[link] if link in new.index else None

        if old_row is None:
            mudanca = "entrou_no_estoque"
            campos = []
        elif new_row is None:
            mudanca = "saiu_do_estoque"
            campos = []
        else:
            campos = [
                campo
                for campo in TRACKED_FIELDS
                if not _same(old_row[campo], new_row[campo])
            ]
            if not campos:
                continue
            mudanca = "alterou"

        row = {
            "link": link,
            "mudanca": mudanca,
            "scrape_date_anterior": old_date,
            "scrape_date_atual": new_date,
            "campos_alterados": ",".join(campos),
        }
        for field in CONTEXT_FIELDS:
            row[field] = _context(old_row, new_row, field)
        for field in TRACKED_FIELDS:
            row[f"{field}_anterior"] = old_row[field] if old_row is not None else None
            row[f"{field}_atual"] = new_row[field] if new_row is not None else None
        rows.append(row)

    result = pd.DataFrame(rows)
    if not result.empty:
        result = result.sort_values(
            ["mudanca", "estado", "cidade", "link"],
            kind="stable",
            na_position="last",
        ).reset_index(drop=True)

    if output is not None:
        destino = Path(output)
        destino.parent.mkdir(parents=True, exist_ok=True)
        result.to_parquet(destino, index=False)
        print(f"Mudanças gravadas em {destino}: {len(result)} registro(s).")

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Compara dois snapshots do Caixa Aberta e grava as mudanças em Parquet."
    )
    parser.add_argument("anterior", help="Parquet do snapshot anterior")
    parser.add_argument("atual", help="Parquet do snapshot atual")
    parser.add_argument(
        "--output",
        default="output_data/mudancas.parquet",
        help="Parquet derivado de saída",
    )
    args = parser.parse_args()
    compare_snapshots(args.anterior, args.atual, args.output)


if __name__ == "__main__":
    main()
