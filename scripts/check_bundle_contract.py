# /// script
# requires-python = ">=3.12"
# dependencies = ["okf-parser==0.45.2"]
# ///
"""Recusa divergência entre o bundle OKF e as constantes do código.

O bundle em `knowledge/` é a fonte de verdade sobre o dataset. Alguns valores
dele estão repetidos no código, porque o pipeline roda em Python 3.10+ e o
okf-parser exige 3.12 — importá-lo em produção custaria o suporte a 3.10/3.11.
A repetição é aceita; a divergência silenciosa, não.

Roda isolado, com as dependências declaradas acima (PEP 723):

    uv run scripts/check_bundle_contract.py

Sai 1 e lista as divergências. Verifica código contra bundle, nunca o
contrário: o bundle não gera código.
"""

from __future__ import annotations

import ast
import json
import sys
from pathlib import Path

from okf_parser import load_bundle

REPO = Path(__file__).resolve().parent.parent
BUNDLE = REPO / "knowledge"

# Colunas que não vêm da fonte normalizada: são acrescentadas pelo próprio
# processamento antes da publicação. Elas continuam fazendo parte do contrato
# do Parquet e podem ser obrigatórias no gate.
PROCESS_DERIVED_COLUMNS = {"latitude", "longitude", "scrape_date"}

_COLLECTIONS = {"frozenset", "set", "tuple", "list"}


def _literal(module: Path, name: str):
    """Lê uma constante de módulo sem importar o módulo.

    Desembrulha `frozenset({...})` e afins: a constante é literal por dentro,
    mesmo quando o módulo a envolve numa chamada.
    """
    tree = ast.parse(module.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        targets = getattr(node, "targets", [])
        if not (
            isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == name for t in targets)
        ):
            continue

        value = node.value
        if (
            isinstance(value, ast.Call)
            and getattr(value.func, "id", None) in _COLLECTIONS
            and len(value.args) == 1
        ):
            value = value.args[0]
        return ast.literal_eval(value)
    raise LookupError(f"{name} não encontrado em {module.relative_to(REPO)}")


def _frontmatter(bundle, concept_type: str) -> list[dict]:
    frame = bundle.concepts.to_pandas()
    rows = frame[frame["concept_type"] == concept_type]
    return [json.loads(value) for value in rows["frontmatter_json"]]


def _one(bundle, concept_type: str) -> dict:
    rows = _frontmatter(bundle, concept_type)
    if len(rows) != 1:
        raise LookupError(
            f"esperado exatamente um conceito {concept_type}, encontrados {len(rows)}"
        )
    return rows[0]


def check_identifier(bundle) -> list[str]:
    """O item é um por ano, o arquivo tem data, e o DDL segue o manifesto."""
    distribution = _one(bundle, "Distribution")
    nomes = REPO / "src" / "archive_names.py"
    problems = []

    prefixo = _literal(nomes, "ITEM_PREFIX")
    if prefixo != distribution["identifier_prefix"]:
        problems.append(
            f"src/archive_names.py: ITEM_PREFIX é {prefixo!r}, "
            f"o bundle declara {distribution['identifier_prefix']!r}"
        )

    ponteiro = _literal(nomes, "MANIFESTO")
    if ponteiro != distribution["manifesto"]:
        problems.append(
            f"src/archive_names.py: MANIFESTO é {ponteiro!r}, "
            f"o bundle declara {distribution['manifesto']!r}"
        )

    ddl = (REPO / "imoveis_caixa.sql").read_text(encoding="utf-8")
    if ponteiro not in ddl:
        problems.append(
            f"imoveis_caixa.sql não lê o manifesto ({ponteiro}): sem ele, o "
            "alvo da view seria derivado do calendário, que não sabe se a "
            "publicação do dia aconteceu"
        )
    if "current_date" in ddl:
        problems.append(
            "imoveis_caixa.sql deriva o alvo de current_date, que depende do "
            "fuso da sessão de quem consulta"
        )

    return problems


def check_required_columns(bundle) -> list[str]:
    """O gate de publicação e o conceito Schema descrevem o mesmo contrato."""
    declared = set(_one(bundle, "Schema")["colunas_obrigatorias"])
    in_code = set(
        _literal(REPO / "src" / "reporter.py", "REQUIRED_PUBLICATION_COLUMNS")
    )
    problems = []

    if declared != in_code:
        problems.append(
            "REQUIRED_PUBLICATION_COLUMNS diverge de colunas_obrigatorias: "
            f"só no código {sorted(in_code - declared)}, "
            f"só no bundle {sorted(declared - in_code)}"
        )

    from_source = set(_literal(REPO / "src" / "fetch_data.py", "CSV_COLUMNS"))
    faltando = sorted(declared - from_source - PROCESS_DERIVED_COLUMNS)
    if faltando:
        problems.append(
            "colunas exigidas para publicar que não vêm da fonte nem são "
            "derivadas pelo processamento: " + ", ".join(faltando)
        )

    return problems


def check_modalidades(bundle) -> list[str]:
    """Toda modalidade que o código conhece tem conceito, e vice-versa."""
    declared = {row["title"] for row in _frontmatter(bundle, "Modalidade")}
    in_code = set(_literal(REPO / "src" / "reporter.py", "KNOWN_MODALIDADES"))
    if declared == in_code:
        return []
    return [
        "KNOWN_MODALIDADES diverge dos conceitos Modalidade: "
        f"só no código {sorted(in_code - declared)}, "
        f"só no bundle {sorted(declared - in_code)}"
    ]


def main() -> int:
    bundle = load_bundle(BUNDLE)
    problems = (
        check_identifier(bundle)
        + check_required_columns(bundle)
        + check_modalidades(bundle)
    )

    if problems:
        print("Bundle e código divergem:")
        for problem in problems:
            print(f"  - {problem}")
        return 1

    print("Bundle e código estão de acordo.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
