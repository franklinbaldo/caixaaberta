from datetime import date

from archive_history import baixar_snapshot, snapshot_anterior
from archive_names import item_do_ano, parquet_datado, url_no_item


DIA = date(2026, 9, 2)


def _metadata_url(identifier):
    return f"https://archive.org/metadata/{identifier}"


def _files(*datas):
    return {"files": [{"name": parquet_datado(data)} for data in datas]}


def test_escolhe_o_maior_snapshot_estritamente_anterior(requests_mock):
    identifier = item_do_ano(DIA)
    requests_mock.get(
        _metadata_url(identifier),
        json={
            "files": [
                {"name": "README.txt"},
                {"name": parquet_datado(date(2026, 8, 31))},
                {"name": parquet_datado(date(2026, 9, 1))},
                {"name": parquet_datado(DIA)},
            ]
        },
    )

    anterior = snapshot_anterior(DIA)

    assert anterior is not None
    assert anterior.data == date(2026, 9, 1)
    assert anterior.item == identifier
    assert anterior.arquivo == parquet_datado(date(2026, 9, 1))


def test_republicar_o_mesmo_dia_nao_compara_o_snapshot_com_ele_mesmo(requests_mock):
    identifier = item_do_ano(DIA)
    requests_mock.get(
        _metadata_url(identifier),
        json=_files(date(2026, 9, 1), DIA),
    )

    assert snapshot_anterior(DIA).data == date(2026, 9, 1)


def test_primeiro_snapshot_do_ano_cai_no_item_anterior(requests_mock):
    primeiro = date(2026, 1, 1)
    requests_mock.get(_metadata_url(item_do_ano(primeiro)), json=_files(primeiro))
    anterior_item = item_do_ano(date(2025, 12, 31))
    requests_mock.get(
        _metadata_url(anterior_item),
        json=_files(date(2025, 12, 30), date(2025, 12, 31)),
    )

    anterior = snapshot_anterior(primeiro)

    assert anterior is not None
    assert anterior.data == date(2025, 12, 31)
    assert anterior.item == anterior_item


def test_sem_historico_retorna_none(requests_mock):
    requests_mock.get(_metadata_url(item_do_ano(DIA)), status_code=404)
    requests_mock.get(
        _metadata_url(item_do_ano(date(2025, 12, 31))), status_code=404
    )

    assert snapshot_anterior(DIA) is None


def test_baixa_exatamente_o_snapshot_descoberto(requests_mock, tmp_path):
    identifier = item_do_ano(DIA)
    anterior_data = date(2026, 9, 1)
    requests_mock.get(
        _metadata_url(identifier),
        json=_files(anterior_data),
    )
    snapshot = snapshot_anterior(DIA)
    conteudo = b"PAR1snapshot-anteriorPAR1"
    requests_mock.get(
        url_no_item(identifier, parquet_datado(anterior_data)), content=conteudo
    )

    destino = baixar_snapshot(snapshot, tmp_path / "anterior.parquet")

    assert destino.read_bytes() == conteudo
