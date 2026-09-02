from datetime import date

from archive_names import item_do_ano, mudancas_datado
from upload_to_archive import publicar_manifesto


DIA = date(2026, 9, 2)
ANTERIOR = date(2026, 9, 1)


def test_manifesto_expoe_derivado_e_base_temporal():
    manifesto = publicar_manifesto(
        DIA,
        item_do_ano(DIA),
        dry_run=True,
        mudancas_desde=ANTERIOR,
    )

    assert manifesto["mudancas_desde"] == ANTERIOR.isoformat()
    assert manifesto["mudancas_url"].endswith(mudancas_datado(DIA))
    assert item_do_ano(DIA) in manifesto["mudancas_url"]


def test_manifesto_antigo_continua_valido_sem_historico():
    manifesto = publicar_manifesto(DIA, item_do_ano(DIA), dry_run=True)

    assert "mudancas_desde" not in manifesto
    assert "mudancas_url" not in manifesto
