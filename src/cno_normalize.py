"""Normalização conservadora das relações abertas do CNO.

Os valores publicados pela Receita são preservados; colunas derivadas existem
apenas para tipagem, consulta e comparação de endereços.
"""

from __future__ import annotations

import argparse
import re
import unicodedata
from pathlib import Path

import pandas as pd

RAW_DIR = Path("cno_data/raw")
OUTPUT_DIR = Path("cno_data/normalized")
TABLES = {
    "cno.csv": "cno.parquet",
    "cno_areas.csv": "cno_areas.parquet",
    "cno_cnaes.csv": "cno_cnaes.parquet",
    "cno_vinculos.csv": "cno_vinculos.parquet",
}
TOTAL_COLUMNS = {
    "total_de_obras": "cno.csv",
    "total_de_cnaes": "cno_cnaes.csv",
    "total_de_areas": "cno_areas.csv",
    "total_de_vinculos": "cno_vinculos.csv",
}
SITUACOES = {
    "01": "NULA",
    "02": "ATIVA",
    "03": "SUSPENSA",
    "14": "PARALISADA",
    "15": "ENCERRADA",
}


class CNOTransformError(RuntimeError):
    """O snapshot bruto não respeita o contrato necessário à normalização."""


def slug(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value))
    ascii_value = "".join(char for char in normalized if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]+", "_", ascii_value.lower()).strip("_")


def normalize_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    normalized = unicodedata.normalize("NFKD", str(value))
    ascii_value = "".join(char for char in normalized if not unicodedata.combining(char))
    return re.sub(r"\s+", " ", ascii_value.upper()).strip()


def normalize_number(value: object) -> str:
    text = normalize_text(value)
    text = re.sub(r"^(?:N[ºO]?|NUMERO)\s*", "", text)
    return re.sub(r"\s+", "", text)


def _read_csv(path: Path) -> pd.DataFrame:
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "latin-1"):
        try:
            frame = pd.read_csv(
                path,
                sep=None,
                engine="python",
                dtype=str,
                encoding=encoding,
                keep_default_na=False,
            )
            frame.columns = [slug(column) for column in frame.columns]
            return frame
        except (UnicodeDecodeError, pd.errors.ParserError) as error:
            last_error = error
    raise CNOTransformError(f"Não foi possível ler {path.name}: {last_error}")


def _parse_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series.replace("", pd.NA), errors="coerce").dt.date


def _parse_decimal(series: pd.Series) -> pd.Series:
    clean = series.astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(clean.replace("", pd.NA), errors="coerce")


def _canonical_cno(frame: pd.DataFrame) -> pd.DataFrame:
    if "cno" not in frame.columns:
        raise CNOTransformError("CNO.CSV não contém a coluna CNO.")

    result = frame.copy()
    result["cno"] = result["cno"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(12)

    for name in (
        "data_de_inicio",
        "data_de_inicio_da_responsabilidade",
        "data_de_registro",
        "data_da_situacao",
    ):
        if name in result.columns:
            result[f"{name}_iso"] = _parse_date(result[name])

    if "area_total" in result.columns:
        result["area_total_num"] = _parse_decimal(result["area_total"])

    if "situacao" in result.columns:
        codes = result["situacao"].astype(str).str.extract(r"(\d{1,2})", expand=False).fillna("")
        result["situacao_codigo"] = codes.str.zfill(2).where(codes.ne(""), "")
        result["situacao_descricao"] = result["situacao_codigo"].map(SITUACOES).fillna("")

    aliases = {
        "estado": "estado_normalizado",
        "nome_do_municipio": "municipio_normalizado",
        "logradouro": "logradouro_normalizado",
        "numero_do_logradouro": "numero_normalizado",
        "bairro": "bairro_normalizado",
        "complemento": "complemento_normalizado",
        "cep": "cep_normalizado",
    }
    for source, target in aliases.items():
        if source not in result.columns:
            result[target] = ""
        elif source == "numero_do_logradouro":
            result[target] = result[source].map(normalize_number)
        elif source == "cep":
            result[target] = result[source].astype(str).str.replace(r"\D", "", regex=True).str.zfill(8)
        else:
            result[target] = result[source].map(normalize_text)

    return result


def _canonical_aux(frame: pd.DataFrame, filename: str) -> pd.DataFrame:
    if "cno" not in frame.columns:
        raise CNOTransformError(f"{filename} não contém a coluna CNO.")
    result = frame.copy()
    result["cno"] = result["cno"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(12)
    for column in ("data_de_registro", "data_de_inicio", "data_de_fim"):
        if column in result.columns:
            result[f"{column}_iso"] = _parse_date(result[column])
    if "metragem" in result.columns:
        result["metragem_num"] = _parse_decimal(result["metragem"])
    return result


def _int_value(value: object) -> int | None:
    digits = re.sub(r"\D", "", str(value))
    return int(digits) if digits else None


def validate_totals(raw_dir: Path, frames: dict[str, pd.DataFrame]) -> None:
    totals = _read_csv(raw_dir / "cno_totais.csv")
    if totals.empty:
        raise CNOTransformError("CNO_TOTAIS.CSV está vazio.")
    row = totals.iloc[0]
    for total_column, filename in TOTAL_COLUMNS.items():
        if total_column not in totals.columns:
            continue
        expected = _int_value(row[total_column])
        if expected is not None and expected != len(frames[filename]):
            raise CNOTransformError(
                f"{filename}: CNO_TOTAIS declara {expected} registros, "
                f"mas foram lidos {len(frames[filename])}."
            )


def normalize_snapshot(
    raw_dir: Path | str = RAW_DIR, output_dir: Path | str = OUTPUT_DIR
) -> dict[str, Path]:
    """Converte as quatro relações úteis do snapshot para Parquet."""
    raw_dir = Path(raw_dir)
    output_dir = Path(output_dir)
    frames = {filename: _read_csv(raw_dir / filename) for filename in TABLES}
    validate_totals(raw_dir, frames)

    normalized = {
        "cno.csv": _canonical_cno(frames["cno.csv"]),
        **{
            filename: _canonical_aux(frame, filename)
            for filename, frame in frames.items()
            if filename != "cno.csv"
        },
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    outputs: dict[str, Path] = {}
    for filename, target_name in TABLES.items():
        target = output_dir / target_name
        normalized[filename].to_parquet(target, index=False)
        outputs[filename] = target
    return outputs


def main() -> None:
    parser = argparse.ArgumentParser(description="Normaliza o snapshot bruto do CNO em Parquet.")
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    args = parser.parse_args()
    outputs = normalize_snapshot(args.raw_dir, args.output_dir)
    for source, target in outputs.items():
        print(f"{source} -> {target}")


if __name__ == "__main__":
    main()
