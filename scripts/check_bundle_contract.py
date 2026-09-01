# /// script
# requires-python = ">=3.12"
# dependencies = ["okf-parser==0.45.2"]
# ///
"""Recusa divergência entre o bundle OKF e as constantes do código.

O bundle em `knowledge/` é a fonte de verdade sobre o dataset. Três valores
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
import re
import sys
from pathlib import Path

from okf_parser import load_bundle

REPO = Path(__file__).resolve().parent.parent
BUNDLE = REPO / "knowledge"


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


def _argparse_default(module: Path, option: str):
    """Lê o default de uma opção de linha de comando sem importar o módulo."""
    tree = ast.parse(module.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if getattr(node.func, "attr", None) != "add_argument":
            continue
        if not any(
            isinstance(a, ast.Constant) and a.value == option for a in node.args
        ):
            continue
        for keyword in node.keywords:
            if keyword.arg == "default":
                return ast.literal_eval(keyword.value)
    raise LookupError(f"default de {option} não encontrado em {module.name}")


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
    """O identificador do item no Archive tem quatro cópias. Uma manda."""
    expected = _one(bundle, "Distribution")["identifier"]
    problems = []

    for module, option in (
        ("run_pipeline.py", "--archive-item-identifier"),
        ("generate_ddl.py", "--identifier"),
    ):
        found = _argparse_default(REPO / "src" / module, option)
        if found != expected:
            problems.append(
                f"src/{module}: default de {option} é {found!r}, "
                f"o bundle declara {expected!r}"
            )

    ddl = (REPO / "imoveis_caixa.sql").read_text(encoding="utf-8")
    urls = re.findall(r"archive\.org/download/([^/]+)/", ddl)
    for found in urls:
        if found != expected:
            problems.append(
                f"imoveis_caixa.sql aponta para o item {found!r}, "
                f"o bundle declara {expected!r}"
            )
    if not urls:
        problems.append("imoveis_caixa.sql não referencia item algum do Archive")

    return problems


def check_required_columns(bundle) -> list[str]:
    """O gate de publicação e o conceito Schema descrevem o mesmo contrato."""
    declared = set(_one(bundle, "Schema")["colunas_obrigatorias"])
    in_code = set(_literal(REPO / "src" / "reporter.py", "REQUIRED_PUBLICATION_COLUMNS"))
    problems = []

    if declared != in_code:
        problems.append(
            "REQUIRED_PUBLICATION_COLUMNS diverge de colunas_obrigatorias: "
            f"só no código {sorted(in_code - declared)}, "
            f"só no bundle {sorted(declared - in_code)}"
        )

    produced = set(_literal(REPO / "src" / "fetch_data.py", "CSV_COLUMNS"))
    faltando = sorted(declared - produced - {"latitude", "longitude"})
    if faltando:
        problems.append(
            "colunas exigidas para publicar que o download não produz: "
            + ", ".join(faltando)
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
