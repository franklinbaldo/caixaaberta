from __future__ import annotations

import pandas as pd
import pytest

from valuation_baseline import (
    infer_property_type,
    load_training_dataset,
    score_frame,
    temporal_benchmark,
    train_model,
)


def _rows(date: str, specs: list[tuple[str, float, float]], *, city: str = "Porto Velho") -> pd.DataFrame:
    rows = []
    for link, price, appraisal in specs:
        rows.append(
            {
                "link": link,
                "estado": "RO",
                "cidade": city,
                "bairro": "Centro",
                "descricao": "Casa residencial com 2 quartos",
                "preco": price,
                "avaliacao": appraisal,
                "scrape_date": date,
            }
        )
    return pd.DataFrame(rows)


def test_infer_property_type_is_coarse_and_deterministic():
    assert infer_property_type("Apartamento, 2 quartos") == "apartamento"
    assert infer_property_type("CASA RESIDENCIAL") == "casa"
    assert infer_property_type("Lote urbano") == "terreno"
    assert infer_property_type(None) == "outro"


def test_training_dataset_uses_first_observation_once_per_property(tmp_path):
    first = _rows("2026-09-01", [("1", 80_000, 100_000), ("2", 90_000, 120_000)])
    second = _rows("2026-09-02", [("1", 70_000, 150_000), ("3", 95_000, 130_000)])
    p1 = tmp_path / "d1.parquet"
    p2 = tmp_path / "d2.parquet"
    first.to_parquet(p1, index=False)
    second.to_parquet(p2, index=False)

    properties = load_training_dataset([p1, p2])

    assert set(properties["link"].astype(str)) == {"1", "2", "3"}
    property_one = properties.loc[properties["link"].astype(str) == "1"].iloc[0]
    assert property_one["avaliacao"] == pytest.approx(100_000)
    assert property_one["scrape_date"] == pd.Timestamp("2026-09-01")


def test_price_is_not_a_feature_of_appraisal_proxy():
    training = _rows(
        "2026-09-01",
        [
            ("1", 10_000, 100_000),
            ("2", 200_000, 110_000),
            ("3", 50_000, 120_000),
            ("4", 500_000, 130_000),
        ],
    )
    model = train_model(training, min_group_size=2)

    low_price = _rows("2026-09-02", [("x", 50_000, 115_000)])
    high_price = low_price.copy()
    high_price["preco"] = 90_000

    low_score = score_frame(low_price, model).iloc[0]
    high_score = score_frame(high_price, model).iloc[0]

    assert low_score["estimativa_avaliacao"] == pytest.approx(115_000)
    assert high_score["estimativa_avaliacao"] == pytest.approx(115_000)
    assert low_score["gap_preco_pct"] > high_score["gap_preco_pct"]


def test_model_prefers_most_specific_group_with_enough_examples():
    training = pd.concat(
        [
            _rows("2026-09-01", [("1", 50_000, 100_000), ("2", 50_000, 120_000)]),
            _rows(
                "2026-09-01",
                [("3", 50_000, 300_000), ("4", 50_000, 320_000)],
                city="Ji-Parana",
            ),
        ],
        ignore_index=True,
    )
    model = train_model(training, min_group_size=2)
    scored = score_frame(_rows("2026-09-02", [("x", 70_000, 110_000)]), model).iloc[0]

    assert scored["escopo_estimativa"] == "estado/cidade/bairro/tipo_imovel"
    assert scored["estimativa_avaliacao"] == pytest.approx(110_000)
    assert scored["n_treino_escopo"] == 2


def test_temporal_benchmark_only_tests_properties_first_seen_later():
    training = _rows(
        "2026-09-01",
        [
            ("t1", 80_000, 100_000),
            ("t2", 90_000, 110_000),
            ("t3", 100_000, 120_000),
            ("t4", 110_000, 130_000),
            ("t5", 120_000, 140_000),
        ],
    )
    holdout = _rows(
        "2026-09-02",
        [
            ("h1", 90_000, 105_000),
            ("h2", 100_000, 115_000),
            ("h3", 110_000, 125_000),
            ("h4", 120_000, 135_000),
            ("h5", 130_000, 145_000),
        ],
    )
    properties = pd.concat([training, holdout], ignore_index=True)

    metrics = temporal_benchmark(
        properties,
        min_group_size=2,
        holdout_fraction=0.20,
        min_segment_size=2,
    )

    global_row = metrics.loc[metrics["nivel"] == "global"].iloc[0]
    assert global_row["n_treino"] == 5
    assert global_row["n_holdout"] == 5
    assert global_row["holdout_desde"] == pd.Timestamp("2026-09-02")
    assert global_row["mae"] >= 0
    assert set(metrics["target"]) == {"avaliacao_caixa_proxy"}


def test_temporal_benchmark_rejects_link_leakage():
    properties = pd.concat(
        [
            _rows("2026-09-01", [("same", 80_000, 100_000), ("a", 90_000, 110_000)]),
            _rows("2026-09-02", [("same", 70_000, 100_000), ("b", 90_000, 120_000)]),
        ],
        ignore_index=True,
    )

    with pytest.raises(ValueError, match="simultaneamente"):
        temporal_benchmark(properties, min_group_size=2)
