"""Matching conservador entre imóveis da Caixa e o Cadastro Nacional de Obras.

O resultado nunca esconde a incerteza: candidatos ficam em tabela separada e
o imóvel só recebe um CNO quando há um vencedor único acima do limiar forte.
"""

from __future__ import annotations

import re
from pathlib import Path

import duckdb
import pandas as pd

from cno_normalize import normalize_text

STRONG_SCORE = 96
STREET_TYPES = (
    (r"^(?:RUA|R\.)\s+", "RUA"),
    (r"^(?:AVENIDA|AV\.)\s+", "AVENIDA"),
    (r"^(?:TRAVESSA|TV\.)\s+", "TRAVESSA"),
    (r"^(?:PRACA|PC\.)\s+", "PRACA"),
    (r"^(?:RODOVIA|ROD\.)\s+", "RODOVIA"),
    (r"^(?:ESTRADA|EST\.)\s+", "ESTRADA"),
    (r"^(?:ALAMEDA|AL\.)\s+", "ALAMEDA"),
)
MATCH_COLUMNS = (
    "cno",
    "cno_match_status",
    "cno_match_score",
    "cno_match_method",
    "cno_match_candidate_count",
    "cno_situacao",
    "cno_data_inicio",
    "cno_area_total",
    "cno_nome_obra",
    "cno_categorias",
    "cno_destinacoes",
    "cno_tipos_obra",
)


def normalize_number(value: object) -> str:
    text = normalize_text(value)
    text = re.sub(r"^(?:N(?:[º°O.]*)|NUMERO)\s*", "", text)
    return re.sub(r"[^A-Z0-9]+", "", text)


def normalize_street(value: object) -> tuple[str, str]:
    """Devolve (logradouro completo, logradouro sem tipo)."""
    text = normalize_text(value)
    for pattern, canonical in STREET_TYPES:
        match = re.match(pattern, text)
        if match:
            bare = text[match.end() :].strip()
            return f"{canonical} {bare}".strip(), bare
    return text, text


def _property_keys(properties: pd.DataFrame) -> pd.DataFrame:
    result = properties.copy().reset_index(drop=True)
    result["_property_row"] = range(len(result))

    addresses = result.get("endereco", pd.Series("", index=result.index)).fillna("").astype(str)
    parts = addresses.str.split(",", n=2, expand=True)
    street_raw = parts[0] if 0 in parts else pd.Series("", index=result.index)
    number_raw = parts[1] if 1 in parts else pd.Series("", index=result.index)

    streets = street_raw.map(normalize_street)
    result["_street_full"] = streets.map(lambda pair: pair[0])
    result["_street_bare"] = streets.map(lambda pair: pair[1])
    result["_number"] = number_raw.map(normalize_number)
    result["_state"] = result.get("estado", pd.Series("", index=result.index)).map(normalize_text)
    result["_city"] = result.get("cidade", pd.Series("", index=result.index)).map(normalize_text)
    result["_bairro"] = result.get("bairro", pd.Series("", index=result.index)).map(normalize_text)
    return result


def _candidates(properties: pd.DataFrame, cno_parquet: Path, conn) -> pd.DataFrame:
    conn.register("caixa_properties", properties)
    return conn.execute(
        """
        SELECT
            p._property_row,
            cast(p.link AS VARCHAR) AS property_id,
            c.cno,
            c.situacao_descricao,
            c.data_de_inicio_iso,
            c.area_total_num,
            c.nome,
            c.bairro_normalizado,
            CASE
                WHEN p._street_full = c.logradouro_completo_normalizado THEN
                    98 + CASE
                        WHEN p._bairro <> '' AND p._bairro = c.bairro_normalizado THEN 2
                        ELSE 0
                    END
                ELSE
                    94 + CASE
                        WHEN p._bairro <> '' AND p._bairro = c.bairro_normalizado THEN 2
                        ELSE 0
                    END
            END AS score,
            CASE
                WHEN p._street_full = c.logradouro_completo_normalizado
                    THEN 'logradouro_completo+numero+municipio+uf'
                ELSE 'logradouro_sem_tipo+numero+municipio+uf'
            END AS method
        FROM caixa_properties p
        JOIN read_parquet(?) c
          ON p._state = c.estado_normalizado
         AND p._city = c.municipio_normalizado
         AND p._number = c.numero_normalizado
        WHERE p._number <> ''
          AND p._state <> ''
          AND p._city <> ''
          AND (
              p._street_full = c.logradouro_completo_normalizado
              OR p._street_bare = c.logradouro_normalizado
          )
        """,
        [str(cno_parquet)],
    ).df()


def _status_candidates(candidates: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if candidates.empty:
        return candidates, pd.DataFrame(
            columns=[
                "_property_row",
                "cno_match_status",
                "cno_match_score",
                "cno_match_method",
                "cno_match_candidate_count",
            ]
        )

    candidates = candidates.sort_values(
        ["_property_row", "score", "cno"], ascending=[True, False, True]
    ).copy()
    candidates["candidate_rank"] = (
        candidates.groupby("_property_row")["score"]
        .rank(method="dense", ascending=False)
        .astype(int)
    )

    summaries = []
    for row_id, group in candidates.groupby("_property_row", sort=False):
        top_score = int(group["score"].max())
        top = group[group["score"] == top_score]
        candidate_count = len(group)
        if len(top) > 1:
            status = "ambiguo"
            winner = None
        elif top_score >= STRONG_SCORE:
            status = "forte"
            winner = top.iloc[0]
        else:
            status = "provavel"
            winner = top.iloc[0]

        summaries.append(
            {
                "_property_row": row_id,
                "cno_match_status": status,
                "cno_match_score": top_score,
                "cno_match_method": winner["method"] if winner is not None else top.iloc[0]["method"],
                "cno_match_candidate_count": candidate_count,
                "_winner_cno": winner["cno"] if status == "forte" and winner is not None else pd.NA,
            }
        )

    summary = pd.DataFrame(summaries)
    candidates = candidates.merge(
        summary[["_property_row", "cno_match_status"]],
        on="_property_row",
        how="left",
    )
    return candidates, summary


def _area_summary(cnos: pd.Series, areas_parquet: Path, conn) -> pd.DataFrame:
    selected = pd.DataFrame({"cno": cnos.dropna().astype(str).unique()})
    if selected.empty or not areas_parquet.exists():
        return pd.DataFrame(columns=["cno", "cno_categorias", "cno_destinacoes", "cno_tipos_obra"])
    conn.register("selected_cnos", selected)
    rows = conn.execute(
        """
        SELECT a.cno, a.categoria, a.destinacao, a.tipo_de_obra
        FROM read_parquet(?) a
        JOIN selected_cnos s USING (cno)
        """,
        [str(areas_parquet)],
    ).df()
    if rows.empty:
        return pd.DataFrame(columns=["cno", "cno_categorias", "cno_destinacoes", "cno_tipos_obra"])

    def joined(values: pd.Series) -> str:
        return " | ".join(sorted({str(value) for value in values if str(value).strip()}))

    return (
        rows.groupby("cno", as_index=False)
        .agg(
            cno_categorias=("categoria", joined),
            cno_destinacoes=("destinacao", joined),
            cno_tipos_obra=("tipo_de_obra", joined),
        )
    )


def match_properties(
    properties: pd.DataFrame,
    cno_parquet: Path | str,
    areas_parquet: Path | str | None = None,
    conn=None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Retorna (imóveis enriquecidos, todos os candidatos auditáveis)."""
    cno_parquet = Path(cno_parquet)
    areas_parquet = Path(areas_parquet) if areas_parquet else cno_parquet.with_name("cno_areas.parquet")
    conn = conn or duckdb.connect()
    keyed = _property_keys(properties)
    candidates, summary = _status_candidates(_candidates(keyed, cno_parquet, conn))

    enriched = keyed.merge(summary, on="_property_row", how="left")
    enriched["cno_match_status"] = enriched["cno_match_status"].fillna("sem_match")
    enriched["cno_match_candidate_count"] = (
        pd.to_numeric(enriched["cno_match_candidate_count"], errors="coerce").fillna(0).astype(int)
    )

    if not candidates.empty:
        winners = candidates.merge(
            summary[["_property_row", "_winner_cno"]], on="_property_row", how="inner"
        )
        winners = winners[winners["cno"] == winners["_winner_cno"]].copy()
        winners = winners.drop_duplicates("_property_row")
        winner_fields = winners[
            [
                "_property_row",
                "cno",
                "situacao_descricao",
                "data_de_inicio_iso",
                "area_total_num",
                "nome",
            ]
        ].rename(
            columns={
                "situacao_descricao": "cno_situacao",
                "data_de_inicio_iso": "cno_data_inicio",
                "area_total_num": "cno_area_total",
                "nome": "cno_nome_obra",
            }
        )
        enriched = enriched.merge(winner_fields, on="_property_row", how="left")
    else:
        for column in ("cno", "cno_situacao", "cno_data_inicio", "cno_area_total", "cno_nome_obra"):
            enriched[column] = pd.NA

    areas = _area_summary(enriched.get("cno", pd.Series(dtype="object")), areas_parquet, conn)
    if not areas.empty:
        enriched = enriched.merge(areas, on="cno", how="left")
    else:
        for column in ("cno_categorias", "cno_destinacoes", "cno_tipos_obra"):
            enriched[column] = pd.NA

    for column in MATCH_COLUMNS:
        if column not in enriched.columns:
            enriched[column] = pd.NA

    internal = [column for column in enriched.columns if column.startswith("_")]
    enriched = enriched.drop(columns=internal)
    return enriched, candidates


def enrich_from_directory(
    properties: pd.DataFrame, cno_dir: Path | str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    cno_dir = Path(cno_dir)
    return match_properties(
        properties,
        cno_dir / "cno.parquet",
        cno_dir / "cno_areas.parquet",
    )
