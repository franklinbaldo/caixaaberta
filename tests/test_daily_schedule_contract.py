from pathlib import Path

import yaml


WORKFLOW = Path(".github/workflows/main.yml")


def _workflow():
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def test_workflow_tem_agendamento_diario():
    workflow = _workflow()
    schedule = workflow["on"]["schedule"]

    assert len(schedule) == 1
    assert schedule[0]["cron"].count("*") == 3


def test_push_nao_publica_snapshot():
    workflow = _workflow()
    publish_if = workflow["jobs"]["publish"]["if"]

    assert "schedule" in publish_if
    assert "workflow_dispatch" in publish_if
    assert "push" not in publish_if


def test_concorrencia_e_so_da_publicacao():
    workflow = _workflow()

    assert "concurrency" not in workflow
    assert workflow["jobs"]["publish"]["concurrency"]["cancel-in-progress"] is False
