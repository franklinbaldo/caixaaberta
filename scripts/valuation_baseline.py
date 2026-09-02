#!/usr/bin/env python3
"""Treina e avalia o baseline próprio de avaliação imobiliária do Caixa Aberta."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from valuation_baseline import (
    load_training_dataset,
    score_frame,
    temporal_benchmark,
    train_model,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Treina o baseline hierárquico usando avaliações da Caixa como proxy, "
            "sem usar o preço mínimo como feature."
        )
    )
    parser.add_argument("snapshots", nargs="+", type=Path)
    parser.add_argument("--model-output", type=Path, required=True)
    parser.add_argument("--metrics-output", type=Path, required=True)
    parser.add_argument("--score-snapshot", type=Path)
    parser.add_argument("--estimates-output", type=Path)
    parser.add_argument("--min-group-size", type=int, default=8)
    parser.add_argument("--holdout-fraction", type=float, default=0.20)
    args = parser.parse_args()

    if bool(args.score_snapshot) != bool(args.estimates_output):
        parser.error("--score-snapshot e --estimates-output devem ser usados juntos")

    properties = load_training_dataset(args.snapshots)
    metrics = temporal_benchmark(
        properties,
        min_group_size=args.min_group_size,
        holdout_fraction=args.holdout_fraction,
    )
    model = train_model(properties, min_group_size=args.min_group_size)

    args.model_output.parent.mkdir(parents=True, exist_ok=True)
    args.metrics_output.parent.mkdir(parents=True, exist_ok=True)
    model.to_parquet(args.model_output, index=False)
    metrics.to_parquet(args.metrics_output, index=False)

    print(f"modelo: {args.model_output} ({len(model)} grupos)")
    print(f"benchmark: {args.metrics_output}")
    global_metrics = metrics.loc[metrics["nivel"] == "global"].iloc[0]
    print(
        "holdout: "
        f"n={int(global_metrics['n'])}, "
        f"MAE=R$ {global_metrics['mae']:,.2f}, "
        f"MdAPE={global_metrics['mediana_ape_pct']:.1f}%, "
        f"WAPE={global_metrics['wape_pct']:.1f}%"
    )

    if args.score_snapshot:
        current = pd.read_parquet(args.score_snapshot)
        estimates = score_frame(current, model)
        args.estimates_output.parent.mkdir(parents=True, exist_ok=True)
        estimates.to_parquet(args.estimates_output, index=False)
        print(f"estimativas: {args.estimates_output} ({len(estimates)} imóveis)")


if __name__ == "__main__":
    main()
