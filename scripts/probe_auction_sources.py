# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb>=1.3", "httpx>=0.28"]
# ///
"""Probe auction evidence already archived by Baliza and CausaGanha.

Downloads public Parquet artifacts from Internet Archive and searches them
locally with DuckDB. The probe is intentionally read-only and reproducible.
"""
from __future__ import annotations

import argparse
import json
import re
import tempfile
from collections import Counter
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import quote

import duckdb
import httpx

IA = "https://archive.org"
TERMS = [
    "leilão", "leilao", "hasta pública", "hasta publica", "praça", "praca",
    "arrematação", "arrematacao", "leiloeiro", "alienação judicial", "alienacao judicial",
]


def get_json(client: httpx.Client, url: str) -> dict:
    r = client.get(url, timeout=90, follow_redirects=True)
    r.raise_for_status()
    return r.json()


def download(client: httpx.Client, url: str, path: Path) -> None:
    with client.stream("GET", url, timeout=300, follow_redirects=True) as r:
        r.raise_for_status()
        with path.open("wb") as f:
            for chunk in r.iter_bytes(1024 * 1024):
                f.write(chunk)


def parquet_text_columns(con: duckdb.DuckDBPyConnection, path: Path) -> list[str]:
    rows = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
    return [r[0] for r in rows if any(t in str(r[1]).upper() for t in ("VARCHAR", "STRING"))]


def search_parquet(path: Path, limit: int = 12) -> tuple[int, list[dict]]:
    con = duckdb.connect()
    try:
        cols = parquet_text_columns(con, path)
        if not cols:
            return 0, []
        haystack = " || ' ' || ".join(f"coalesce(cast(\"{c}\" as varchar),'')" for c in cols)
        regex = "(?i)(" + "|".join(re.escape(t) for t in TERMS) + ")"
        total = con.execute(
            f"SELECT count(*) FROM read_parquet(?) WHERE regexp_matches({haystack}, ?)",
            [str(path), regex],
        ).fetchone()[0]
        rows = con.execute(
            f"SELECT * FROM read_parquet(?) WHERE regexp_matches({haystack}, ?) LIMIT ?",
            [str(path), regex, limit],
        ).fetchdf()
        samples = []
        for record in rows.to_dict("records"):
            compact = {k: str(v)[:500] for k, v in record.items() if v is not None and str(v) not in ("", "nan", "None")}
            samples.append(compact)
        return total, samples
    finally:
        con.close()


def ia_files(client: httpx.Client, item: str) -> list[str]:
    meta = get_json(client, f"{IA}/metadata/{item}")
    return [f["name"] for f in meta.get("files", []) if f.get("name", "").endswith(".parquet")]


def probe_baliza(client: httpx.Client, work: Path, months: list[str]) -> dict:
    out = {"items": [], "matches": 0, "samples": []}
    for month in months:
        item = f"baliza-pncp-{month}"
        files = ia_files(client, item)
        selected = [f for f in files if any(x in f.lower() for x in ("public", "contrat", "edital"))] or files
        item_result = {"item": item, "parquets": [], "matches": 0}
        for i, name in enumerate(selected[:8]):
            local = work / f"baliza-{month}-{i}.parquet"
            url = f"{IA}/download/{item}/{quote(name)}"
            download(client, url, local)
            count, samples = search_parquet(local)
            item_result["parquets"].append({"name": name, "rows_matching": count})
            item_result["matches"] += count
            out["matches"] += count
            out["samples"].extend(samples[:4])
        out["items"].append(item_result)
    return out


def causa_recent_parquets(client: httpx.Client, since: date) -> list[tuple[str, str, str]]:
    # CausaGanha publishes a queryable manifest that already knows item, filename and date.
    manifest_url = f"{IA}/download/causaganha-catalog/manifest.parquet"
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "manifest.parquet"
        download(client, manifest_url, p)
        con = duckdb.connect()
        try:
            cols = {r[0] for r in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(p)]).fetchall()}
            wanted = {"item_id", "item", "filename", "date", "file_type", "table_name"}
            if not ({"filename", "date"} <= cols):
                raise RuntimeError(f"unexpected CausaGanha manifest schema: {sorted(cols)}")
            item_col = "item_id" if "item_id" in cols else "item"
            table_pred = "lower(coalesce(table_name,''))='comunicacoes'" if "table_name" in cols else "lower(filename) like '%comunicacoes%parquet%'"
            rows = con.execute(
                f"SELECT {item_col}, filename, cast(date as varchar) FROM read_parquet(?) "
                f"WHERE file_type='parquet' AND {table_pred} AND cast(date as date) >= ? ORDER BY date DESC",
                [str(p), since.isoformat()],
            ).fetchall()
            return [(str(a), str(b), str(c)) for a, b, c in rows]
        finally:
            con.close()


def probe_causaganha(client: httpx.Client, work: Path, since: date, max_files: int) -> dict:
    candidates = causa_recent_parquets(client, since)
    out = {"candidate_parquets": len(candidates), "scanned_parquets": 0, "matches": 0, "by_date": Counter(), "samples": []}
    for i, (item, name, d) in enumerate(candidates[:max_files]):
        local = work / f"causa-{i}.parquet"
        download(client, f"{IA}/download/{item}/{quote(name)}", local)
        count, samples = search_parquet(local)
        out["scanned_parquets"] += 1
        out["matches"] += count
        out["by_date"][d] += count
        for s in samples[:3]:
            s["_source_item"] = item
            s["_source_file"] = name
            s["_source_date"] = d
            out["samples"].append(s)
    out["by_date"] = dict(out["by_date"])
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=62)
    ap.add_argument("--causa-max-files", type=int, default=20, help="bound downloads while sampling the two-month corpus")
    ap.add_argument("--output", default="probe-results.json")
    args = ap.parse_args()
    today = date.today()
    since = today - timedelta(days=args.days)
    months = sorted({since.strftime("%Y-%m"), today.strftime("%Y-%m")})
    with httpx.Client(headers={"User-Agent": "caixaaberta-auction-probe/0.1"}) as client, tempfile.TemporaryDirectory() as td:
        work = Path(td)
        result = {
            "generated_on": today.isoformat(),
            "window": {"since": since.isoformat(), "until": today.isoformat()},
            "terms": TERMS,
            "baliza": probe_baliza(client, work, months),
            "causaganha": probe_causaganha(client, work, since, args.causa_max_files),
        }
    Path(args.output).write_text(json.dumps(result, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
