from __future__ import annotations

import math
import re
import unicodedata
from pathlib import Path
from typing import Iterable

import pandas as pd

MODEL_FAMILY = "caixa-avaliacao-proxy-hmedian-v0"
TARGET = "avaliacao"
MISSING = "__AUSENTE__"

FEATURE_COLUMNS = ("estado", "cidade", "bairro", "tipo_imovel")
SCOPE_LEVELS = (
    ("estado", "cidade", "bairro", "tipo_imovel"),
    ("estado", "cidade", "tipo_imovel"),
    ("estado", "cidade", "bairro"),
    ("estado", "cidade"),
    ("estado", "tipo_imovel"),
    ("estado",),
    (),
)

TRAINING_REQUIRED = {
    "link",
    "estado",
    "cidade",
    "bairro",
    "descricao",
    "preco",
    "avaliacao",
    "scrape_date",
}


def _ascii_upper(value: object) -> str:
    if value is None or pd.isna(value):
        return MISSING
    text = str(value).strip()
    if not text:
        return MISSING
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    return re.sub(r"\s+", " ", text).upper()


def infer_property_type(description: object) -> str:
    """Extrai um tipo grosseiro e estável sem usar preço ou avaliação."""
    text = _ascii_upper(description)
    rules = (
        ("apartamento", ("APARTAMENTO",)),
        ("casa", ("CASA", "RESIDENCIA")),
        ("terreno", ("TERRENO", "LOTE")),
        ("sala", ("SALA COMERCIAL", "SALA")),
        ("loja", ("LOJA",)),
        ("galpao", ("GALPAO",)),
        ("predio", ("PREDIO", "EDIFICIO")),
        ("rural", ("FAZENDA", "SITIO", "CHACARA", "IMOVEL RURAL")),
    )
    for kind, needles in rules:
        if any(needle in text for needle in needles):
            return kind
    return "outro"


def _prepare_frame(frame: pd.DataFrame, *, require_target: bool) -> pd.DataFrame:
    required = {"link", "estado", "cidade", "bairro", "descricao", "preco"}
    if require_target:
        required |= {"avaliacao", "scrape_date"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"colunas ausentes para estimativa: {', '.join(missing)}")

    prepared = frame.copy()
    for column in ("estado", "cidade", "bairro"):
        prepared[column] = prepared[column].map(_ascii_upper)
    prepared["tipo_imovel"] = prepared["descricao"].map(infer_property_type)
    prepared["preco"] = pd.to_numeric(prepared["preco"], errors="coerce")

    if "avaliacao" in prepared.columns:
        prepared["avaliacao"] = pd.to_numeric(prepared["avaliacao"], errors="coerce")
    if "scrape_date" in prepared.columns:
        prepared["scrape_date"] = pd.to_datetime(
            prepared["scrape_date"], errors="coerce"
        ).dt.normalize()

    prepared["link"] = prepared["link"].astype("string")
    return prepared


def load_training_dataset(paths: Iterable[str | Path]) -> pd.DataFrame:
    """Lê snapshots e retorna uma observação por imóvel, na primeira aparição.

    Um imóvel que permanece publicado por muitos dias não ganha peso artificial
    no treino. A primeira aparição também permite holdout temporal por imóveis
    efetivamente novos, sem o mesmo ``link`` vazar para treino e teste.
    """
    frames: list[pd.DataFrame] = []
    for raw_path in paths:
        path = Path(raw_path)
        frame = pd.read_parquet(path)
        missing = sorted(TRAINING_REQUIRED - set(frame.columns))
        if missing:
            raise ValueError(f"{path}: colunas ausentes: {', '.join(missing)}")
        frames.append(frame)

    if not frames:
        raise ValueError("nenhum snapshot informado")

    prepared = _prepare_frame(pd.concat(frames, ignore_index=True), require_target=True)
    prepared = prepared[
        prepared["link"].notna()
        & prepared["scrape_date"].notna()
        & prepared["avaliacao"].notna()
        & (prepared["avaliacao"] > 0)
    ].copy()
    if prepared.empty:
        raise ValueError("nenhuma observação válida para treinar")

    prepared = prepared.sort_values(["scrape_date", "link"], kind="stable")
    prepared = prepared.drop_duplicates(subset=["link"], keep="first")
    return prepared.reset_index(drop=True)


def _scope_name(columns: tuple[str, ...]) -> str:
    return "/".join(columns) if columns else "global"


def train_model(frame: pd.DataFrame, *, min_group_size: int = 8) -> pd.DataFrame:
    """Treina um baseline hierárquico por medianas da avaliação da Caixa.

    ``preco`` deliberadamente não entra nas features. Assim o residual entre
    estimativa e preço pedido continua sendo uma comparação, não uma tautologia.
    """
    if min_group_size < 2:
        raise ValueError("min_group_size deve ser >= 2")

    prepared = _prepare_frame(frame, require_target=True)
    prepared = prepared[
        prepared["avaliacao"].notna() & (prepared["avaliacao"] > 0)
    ].copy()
    if prepared.empty:
        raise ValueError("nenhuma avaliação válida para treinar")

    trained_through = prepared["scrape_date"].max()
    if pd.isna(trained_through):
        raise ValueError("scrape_date inválido no conjunto de treino")
    model_id = (
        f"{MODEL_FAMILY}@{trained_through.date().isoformat()}"
        f"+n{len(prepared)}"
    )

    rows: list[dict[str, object]] = []
    for rank, columns in enumerate(SCOPE_LEVELS):
        if not columns:
            groups = [((), prepared)]
        else:
            groups = prepared.groupby(list(columns), dropna=False, sort=True)

        for key, group in groups:
            if columns and len(group) < min_group_size:
                continue
            if not isinstance(key, tuple):
                key = (key,)

            target = group["avaliacao"].astype(float)
            row: dict[str, object] = {
                "model_id": model_id,
                "model_family": MODEL_FAMILY,
                "target": "avaliacao_caixa_proxy",
                "scope_rank": rank,
                "scope": _scope_name(columns),
                "n_treino": len(group),
                "estimativa_avaliacao": float(target.median()),
                "faixa_inferior": float(target.quantile(0.10)),
                "faixa_superior": float(target.quantile(0.90)),
                "trained_through": trained_through,
            }
            row.update({column: None for column in FEATURE_COLUMNS})
            for column, value in zip(columns, key):
                row[column] = value
            rows.append(row)

    model = pd.DataFrame(rows)
    if model.empty or not (model["scope"] == "global").any():
        raise AssertionError("modelo sem fallback global")
    return model.sort_values(["scope_rank", "scope"], kind="stable").reset_index(drop=True)


def _model_lookups(model: pd.DataFrame) -> dict[str, dict[tuple[object, ...], dict[str, object]]]:
    lookups: dict[str, dict[tuple[object, ...], dict[str, object]]] = {}
    for columns in SCOPE_LEVELS:
        scope = _scope_name(columns)
        subset = model[model["scope"] == scope]
        bucket: dict[tuple[object, ...], dict[str, object]] = {}
        for _, row in subset.iterrows():
            key = tuple(row[column] for column in columns)
            bucket[key] = row.to_dict()
        lookups[scope] = bucket
    return lookups


def score_frame(frame: pd.DataFrame, model: pd.DataFrame) -> pd.DataFrame:
    """Aplica o baseline e calcula a distância entre proxy e preço da Caixa."""
    prepared = _prepare_frame(frame, require_target=False)
    lookups = _model_lookups(model)
    output: list[dict[str, object]] = []

    for _, source in prepared.iterrows():
        chosen: dict[str, object] | None = None
        for columns in SCOPE_LEVELS:
            scope = _scope_name(columns)
            key = tuple(source[column] for column in columns)
            chosen = lookups[scope].get(key)
            if chosen is not None:
                break
        if chosen is None:
            raise AssertionError("modelo sem fallback aplicável")

        estimate = float(chosen["estimativa_avaliacao"])
        price = source.get("preco")
        price_float = float(price) if pd.notna(price) else math.nan
        gap_abs = estimate - price_float if math.isfinite(price_float) else math.nan
        gap_pct = (gap_abs / estimate * 100.0) if estimate > 0 and math.isfinite(gap_abs) else math.nan

        actual = source.get("avaliacao")
        actual_float = float(actual) if pd.notna(actual) else math.nan
        ape = (
            abs(estimate - actual_float) / actual_float * 100.0
            if math.isfinite(actual_float) and actual_float > 0
            else math.nan
        )

        output.append(
            {
                "link": source["link"],
                "scrape_date": source.get("scrape_date"),
                "estado": source["estado"],
                "cidade": source["cidade"],
                "bairro": source["bairro"],
                "tipo_imovel": source["tipo_imovel"],
                "preco": price_float,
                "avaliacao": actual_float,
                "model_id": chosen["model_id"],
                "target": chosen["target"],
                "escopo_estimativa": chosen["scope"],
                "n_treino_escopo": int(chosen["n_treino"]),
                "estimativa_avaliacao": estimate,
                "faixa_inferior": float(chosen["faixa_inferior"]),
                "faixa_superior": float(chosen["faixa_superior"]),
                "gap_preco_abs": gap_abs,
                "gap_preco_pct": gap_pct,
                "erro_vs_avaliacao_pct": ape,
            }
        )

    return pd.DataFrame(output)


def _metrics_row(scored: pd.DataFrame, *, level: str, estado: str | None, cidade: str | None) -> dict[str, object]:
    errors = (scored["estimativa_avaliacao"] - scored["avaliacao"]).abs()
    target = scored["avaliacao"].astype(float)
    ape = errors / target * 100.0
    coverage = (
        (target >= scored["faixa_inferior"])
        & (target <= scored["faixa_superior"])
    )
    return {
        "nivel": level,
        "estado": estado,
        "cidade": cidade,
        "n": len(scored),
        "mae": float(errors.mean()),
        "mediana_ape_pct": float(ape.median()),
        "wape_pct": float(errors.sum() / target.sum() * 100.0),
        "cobertura_faixa_pct": float(coverage.mean() * 100.0),
    }


def temporal_benchmark(
    properties: pd.DataFrame,
    *,
    min_group_size: int = 8,
    holdout_fraction: float = 0.20,
    min_segment_size: int = 5,
) -> pd.DataFrame:
    """Avalia propriedades novas em datas posteriores às usadas no treino."""
    if not 0 < holdout_fraction < 1:
        raise ValueError("holdout_fraction deve estar entre 0 e 1")

    prepared = _prepare_frame(properties, require_target=True)
    prepared = prepared[
        prepared["avaliacao"].notna()
        & (prepared["avaliacao"] > 0)
        & prepared["scrape_date"].notna()
    ].copy()
    dates = sorted(prepared["scrape_date"].drop_duplicates().tolist())
    if len(dates) < 2:
        raise ValueError("benchmark temporal exige pelo menos duas datas distintas")

    holdout_dates = max(1, math.ceil(len(dates) * holdout_fraction))
    holdout_dates = min(holdout_dates, len(dates) - 1)
    cutoff = dates[-holdout_dates]
    train = prepared[prepared["scrape_date"] < cutoff]
    test = prepared[prepared["scrape_date"] >= cutoff]
    if train.empty or test.empty:
        raise ValueError("split temporal produziu treino ou holdout vazio")

    train_links = set(train["link"].astype(str))
    test_links = set(test["link"].astype(str))
    if train_links & test_links:
        raise ValueError("link presente simultaneamente em treino e holdout")

    model = train_model(train, min_group_size=min_group_size)
    scored = score_frame(test, model)
    rows = [_metrics_row(scored, level="global", estado=None, cidade=None)]

    for estado, group in scored.groupby("estado", sort=True):
        if len(group) >= min_segment_size:
            rows.append(_metrics_row(group, level="estado", estado=estado, cidade=None))
    for (estado, cidade), group in scored.groupby(["estado", "cidade"], sort=True):
        if len(group) >= min_segment_size:
            rows.append(_metrics_row(group, level="cidade", estado=estado, cidade=cidade))

    result = pd.DataFrame(rows)
    result["model_id"] = model.iloc[0]["model_id"]
    result["target"] = "avaliacao_caixa_proxy"
    result["treino_ate"] = train["scrape_date"].max()
    result["holdout_desde"] = cutoff
    result["n_treino"] = len(train)
    result["n_holdout"] = len(test)
    return result
