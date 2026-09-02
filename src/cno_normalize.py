"""Normalização conservadora e em streaming das relações abertas do CNO.

O CNO tem milhões de registros. Os CSVs são processados em lotes e escritos
incrementalmente em Parquet para não materializar a base inteira em memória.
Valores publicados pela Receita são preservados; colunas derivadas existem
apenas para tipagem, consulta e comparação de endereços.
"""

from __future__ import annotations

import argparse
import csv
import re
import shutil
import tempfile
import unicodedata
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

RAW_DIR = Path("cno_data/raw")
OUTPUT_DIR = Path("cno_data/normalized")
CHUNK_SIZE = 100_000
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


def _csv_options(path: Path) -> tuple[str, str]:
    sample = path.read_bytes()[:65_536]
    try:
        text = sample.decode("utf-8-sig")
        encoding = "utf-8-sig"
    except UnicodeDecodeError:
        text = sample.decode("latin-1")
        encoding = "latin-1"

    try:
        delimiter = csv.Sniffer().sniff(text, delimiters=";,|").delimiter
    except csv.Error:
        first_line = text.splitlines()[0] if text.splitlines() else ""
        delimiter = ";" if first_line.count(";") >= first_line.count(",") else ","
    return encoding, delimiter


def _iter_csv(path: Path, chunk_size: int = CHUNK_SIZE) -> Iterator[pd.DataFrame]:
    encoding, delimiter = _csv_options(path)
    reader = pd.read_csv(
        path,
        sep=delimiter,
        engine="c",
        dtype=str,
        encoding=encoding,
        keep_default_na=False,
        chunksize=chunk_size,
    )
    for frame in reader:
        frame.columns = [slug(column) for column in frame.columns]
        yield frame


def _read_small_csv(path: Path) -> pd.DataFrame:
    chunks = list(_iter_csv(path, chunk_size=10_000))
    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


def _parse_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series.replace("", pd.NA), errors="coerce").dt.date


def _parse_decimal(series: pd.Series) -> pd.Series:
    def parse(value: object):
        text = str(value).strip()
        if not text:
            return pd.NA
        if "," in text:
            text = text.replace(".", "").replace(",", ".")
        return pd.to_numeric(text, errors="coerce")

    return series.map(parse)


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
        "tipo_de_logradouro": "tipo_logradouro_normalizado",
        "logradouro": "logradouro_normalizado",
        "numero_do_logradouro": "numero_normalizado",
        "bairro": "bairro_normalizado",
        "complemento": "complemento_normalizado",
    }
    for source, target in aliases.items():
        if source not in result.columns:
            result[target] = ""
        elif source == "numero_do_logradouro":
            result[target] = result[source].map(normalize_number)
        else:
            result[target] = result[source].map(normalize_text)

    if "cep" in result.columns:
        cep = result["cep"].astype(str).str.replace(r"\D", "", regex=True)
        result["cep_normalizado"] = cep.str.zfill(8).where(cep.ne(""), "")
    else:
        result["cep_normalizado"] = ""

    result["logradouro_completo_normalizado"] = (
        result["tipo_logradouro_normalizado"] + " " + result["logradouro_normalizado"]
    ).str.strip()
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


def _expected_totals(raw_dir: Path) -> dict[str, int]:
    totals = _read_small_csv(raw_dir / "cno_totais.csv")
    if totals.empty:
        raise CNOTransformError("CNO_TOTAIS.CSV está vazio.")
    row = totals.iloc[0]
    expected: dict[str, int] = {}
    for total_column, filename in TOTAL_COLUMNS.items():
        if total_column in totals.columns:
            value = _int_value(row[total_column])
            if value is not None:
                expected[filename] = value
    return expected


def _write_stream(path: Path, source: Path, filename: str) -> int:
    writer: pq.ParquetWriter | None = None
    count = 0
    try:
        for chunk in _iter_csv(source):
            normalized = (
                _canonical_cno(chunk)
                if filename == "cno.csv"
                else _canonical_aux(chunk, filename)
            )
            table = pa.Table.from_pandas(normalized, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(path, table.schema, compression="zstd")
            writer.write_table(table)
            count += len(normalized)
    finally:
        if writer is not None:
            writer.close()
    if writer is None:
        raise CNOTransformError(f"{filename} está vazio.")
    return count


def normalize_snapshot(
    raw_dir: Path | str = RAW_DIR, output_dir: Path | str = OUTPUT_DIR
) -> dict[str, Path]:
    """Converte as relações úteis do snapshot para Parquet com memória limitada."""
    raw_dir = Path(raw_dir)
    output_dir = Path(output_dir)
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    expected = _expected_totals(raw_dir)

    with tempfile.TemporaryDirectory(prefix="cno-normalized-", dir=output_dir.parent) as temp:
        stage = Path(temp) / "snapshot"
        stage.mkdir()
        counts: dict[str, int] = {}
        for filename, target_name in TABLES.items():
            counts[filename] = _write_stream(stage / target_name, raw_dir / filename, filename)

        for filename, declared in expected.items():
            actual = counts[filename]
            if actual != declared:
                raise CNOTransformError(
                    f"{filename}: CNO_TOTAIS declara {declared} registros, "
                    f"mas foram lidos {actual}."
                )

        installed = Path(temp) / "installed"
        stage.replace(installed)
        if output_dir.exists():
            shutil.rmtree(output_dir)
        shutil.copytree(installed, output_dir)

    return {filename: output_dir / target for filename, target in TABLES.items()}


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
