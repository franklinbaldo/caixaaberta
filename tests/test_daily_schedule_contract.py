from pathlib import Path


WORKFLOW = Path(".github/workflows/main.yml")


def _texto():
    return WORKFLOW.read_text(encoding="utf-8")


def test_workflow_tem_agendamento_diario():
    texto = _texto()

    assert "schedule:" in texto
    assert 'cron: "17 6 * * *"' in texto


def test_push_nao_publica_snapshot():
    texto = _texto()
    publish = texto.split("\n  publish:\n", 1)[1]

    assert "github.event_name == 'schedule'" in publish
    assert "github.event_name == 'workflow_dispatch'" in publish
    assert "github.event_name == 'push'" not in publish


def test_concorrencia_e_so_da_publicacao():
    texto = _texto()
    antes_publish, publish = texto.split("\n  publish:\n", 1)

    assert "concurrency:" not in antes_publish
    assert "concurrency:" in publish
    assert "cancel-in-progress: false" in publish
