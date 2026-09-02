# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb>=1.3", "httpx>=0.28"]
# ///
"""Mede, sem alterar o pipeline, o ganho potencial de usar número no CNEFE.

Baixa o último snapshot público do Caixa Aberta e a tabela estável do CNEFE
que contém número. O objetivo é responder antes de aumentar o custo diário:
quantos anúncios têm número útil, quantos casam exatamente e quanto custa ler a
referência de ~616 MB.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import duckdb
import httpx

IA_MANIFEST = (
    "https://archive.org/download/imoveis-caixa-economica-federal/latest.json"
)
LEGACY_SNAPSHOT = (
    "https://archive.org/download/imoveis-caixa-economica-federal/"
    "imoveis_geocoded.parquet"
)
CNEFE_NUMERO = (
    "https://github.com/ipeaGIT/padronizacao_cnefe/releases/download/v0.4.1/"
    "municipio_logradouro_numero_localidade.parquet"
)
EXPECTED_CNEFE_COLUMNS = {
    "estado",
    "municipio",
    "logradouro",
    "numero",
    "localidade",
    "lat",
    "lon",
}

ABREVIACOES = (
    (r"^AV\.? ", "AVENIDA "),
    (r"^R\.? ", "RUA "),
    (r"^TV\.? ", "TRAVESSA "),
    (r"^PC\.? ", "PRACA "),
    (r"^ROD\.? ", "RODOVIA "),
    (r"^EST\.? ", "ESTRADA "),
    (r"^AL\.? ", "ALAMEDA "),
)


def normalizado(expressao: str) -> str:
    sql = f"upper(strip_accents(trim({expressao})))"
    for padrao, troca in ABREVIACOES:
        sql = f"regexp_replace({sql}, '{padrao}', '{troca}')"
    return f"regexp_replace({sql}, '\\s+', ' ', 'g')"


def download(client: httpx.Client, url: str, destino: Path) -> tuple[int, float]:
    inicio = time.monotonic()
    total = 0
    with client.stream("GET", url, timeout=600, follow_redirects=True) as response:
        response.raise_for_status()
        with destino.open("wb") as output:
            for chunk in response.iter_bytes(1024 * 1024):
                output.write(chunk)
                total += len(chunk)
    return total, time.monotonic() - inicio


def snapshot_url(client: httpx.Client) -> str:
    response = client.get(IA_MANIFEST, timeout=30, follow_redirects=True)
    if response.status_code == 200:
        payload = response.json()
        url = payload.get("parquet_url")
        if url:
            return str(url)
    return LEGACY_SNAPSHOT


def benchmark(snapshot: Path, cnefe: Path) -> dict:
    con = duckdb.connect()
    try:
        schema = con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(cnefe)]
        ).fetchall()
        cols = {row[0] for row in schema}
        missing = sorted(EXPECTED_CNEFE_COLUMNS - cols)
        if missing:
            raise RuntimeError(
                "schema inesperado da tabela CNEFE com número; faltam: "
                + ", ".join(missing)
            )

        inicio = time.monotonic()
        query = f"""
        WITH imoveis AS (
            SELECT
                row_number() OVER () AS id,
                {normalizado('estado')} AS estado,
                {normalizado('cidade')} AS municipio,
                {normalizado("split_part(endereco, ',', 1)")} AS logradouro,
                {normalizado("coalesce(bairro, '')")} AS localidade,
                try_cast(
                    regexp_extract(endereco, '(?i)\\bN\\.?\\s*([0-9]+)', 1)
                    AS BIGINT
                ) AS numero,
                precisao
            FROM read_parquet(?)
        ),
        cnefe AS (
            SELECT
                {normalizado('estado')} AS estado,
                {normalizado('municipio')} AS municipio,
                {normalizado('logradouro')} AS logradouro,
                {normalizado("coalesce(localidade, '')")} AS localidade,
                try_cast(numero AS BIGINT) AS numero
            FROM read_parquet(?)
            WHERE try_cast(numero AS BIGINT) IS NOT NULL
        ),
        estrito AS (
            SELECT DISTINCT i.id
            FROM imoveis i
            JOIN cnefe c USING (estado, municipio, logradouro, localidade, numero)
            WHERE i.numero > 0
        ),
        relaxado AS (
            SELECT DISTINCT i.id
            FROM imoveis i
            JOIN cnefe c USING (estado, municipio, logradouro, numero)
            WHERE i.numero > 0
        )
        SELECT
            count(*) AS total,
            count(*) FILTER (WHERE numero IS NOT NULL) AS numero_extraido,
            count(*) FILTER (WHERE numero = 0) AS numero_zero,
            count(*) FILTER (WHERE numero > 0) AS numero_util,
            count(*) FILTER (
                WHERE precisao IN ('logradouro_localidade', 'logradouro')
            ) AS ja_no_nivel_de_logradouro,
            (SELECT count(*) FROM estrito) AS match_numero_com_localidade,
            (SELECT count(*) FROM relaxado) AS match_numero_sem_localidade,
            count(*) FILTER (
                WHERE id IN (SELECT id FROM relaxado)
                  AND coalesce(precisao, '') NOT IN (
                      'logradouro_localidade', 'logradouro'
                  )
            ) AS cobertura_nova_potencial
        FROM imoveis
        """
        row = con.execute(query, [str(snapshot), str(cnefe)]).fetchone()
        elapsed = time.monotonic() - inicio
        names = [desc[0] for desc in con.description]
        metrics = dict(zip(names, row))
        metrics["query_seconds"] = round(elapsed, 3)

        total = metrics["total"] or 1
        util = metrics["numero_util"] or 1
        metrics["pct_numero_util"] = round(metrics["numero_util"] / total * 100, 2)
        metrics["pct_match_estrito_dos_uteis"] = round(
            metrics["match_numero_com_localidade"] / util * 100, 2
        )
        metrics["pct_match_relaxado_dos_uteis"] = round(
            metrics["match_numero_sem_localidade"] / util * 100, 2
        )
        return metrics
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="cnefe-number-benchmark.json")
    parser.add_argument("--snapshot-url")
    args = parser.parse_args()

    work = Path(".benchmark-cnefe")
    work.mkdir(exist_ok=True)
    snapshot = work / "snapshot.parquet"
    cnefe = work / "municipio_logradouro_numero_localidade.parquet"

    with httpx.Client(headers={"User-Agent": "caixaaberta-cnefe-benchmark/1"}) as client:
        source = args.snapshot_url or snapshot_url(client)
        snapshot_bytes, snapshot_seconds = download(client, source, snapshot)
        cnefe_bytes, cnefe_seconds = download(client, CNEFE_NUMERO, cnefe)

    result = {
        "snapshot_url": source,
        "cnefe_url": CNEFE_NUMERO,
        "snapshot_bytes": snapshot_bytes,
        "snapshot_download_seconds": round(snapshot_seconds, 3),
        "cnefe_bytes": cnefe_bytes,
        "cnefe_download_seconds": round(cnefe_seconds, 3),
        **benchmark(snapshot, cnefe),
    }
    Path(args.output).write_text(
        json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
