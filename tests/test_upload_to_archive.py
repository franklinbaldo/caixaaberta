import sys
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from archive_names import (
    ITEM_PONTEIRO,
    MANIFESTO,
    bruto_datado,
    item_do_ano,
    parquet_datado,
)
from upload_to_archive import (
    artefatos_da_publicacao,
    publicar_manifesto,
    upload_files_to_archive,
)


@pytest.fixture
def sem_manifesto(monkeypatch):
    """O Archive ainda não tem ponteiro: primeira publicação."""
    monkeypatch.setattr("upload_to_archive.manifesto_publicado", lambda: None)


def _com_manifesto(monkeypatch, data):
    monkeypatch.setattr("upload_to_archive.manifesto_publicado", lambda: {"data": data})


QUANDO = date(2026, 9, 2)


@pytest.fixture
def mock_env(monkeypatch):
    monkeypatch.setenv("IA_ACCESS_KEY", "test_access_key")
    monkeypatch.setenv("IA_SECRET_KEY", "test_secret_key")


@pytest.fixture
def publicacao(tmp_path):
    """Um diretório com a publicação de QUANDO, e lixo ao redor."""
    directory = tmp_path / "output_data"
    directory.mkdir()
    (directory / parquet_datado(QUANDO)).write_text("dummy")
    (directory / bruto_datado(QUANDO)).write_text("dummy")
    (directory / "ignore.txt").write_text("ignore")
    return directory


# --- O par (data, parquet, bruto) ------------------------------------------


def test_a_publicacao_e_o_par_do_dia(publicacao):
    artefatos = [Path(f).name for f in artefatos_da_publicacao(QUANDO, publicacao)]

    assert set(artefatos) == {parquet_datado(QUANDO), bruto_datado(QUANDO)}


def test_o_diretorio_sujo_nao_contamina_a_publicacao(publicacao):
    """Retratos de outros dias no diretório não sobem junto.

    Varrer o diretório mandaria retratos velhos para o item novo na virada do
    ano, e passaria pelo gate com o Parquet de hoje e o bruto de ontem.
    """
    ontem = date(2026, 9, 1)
    (publicacao / parquet_datado(ontem)).write_text("velho")
    (publicacao / bruto_datado(ontem)).write_text("velho")
    (publicacao / parquet_datado(date(2025, 12, 31))).write_text("ano passado")

    artefatos = [Path(f).name for f in artefatos_da_publicacao(QUANDO, publicacao)]

    assert set(artefatos) == {parquet_datado(QUANDO), bruto_datado(QUANDO)}


def test_o_bruto_de_outro_dia_nao_satisfaz_o_gate(tmp_path):
    """Proveniência é o bruto *daquele* dia, não um zip com o prefixo certo."""
    directory = tmp_path / "output_data"
    directory.mkdir()
    (directory / parquet_datado(QUANDO)).write_text("dummy")
    (directory / bruto_datado(date(2026, 9, 1))).write_text("de ontem")

    with pytest.raises(FileNotFoundError, match=bruto_datado(QUANDO)):
        artefatos_da_publicacao(QUANDO, directory)


def test_um_zip_qualquer_nao_satisfaz_o_gate(tmp_path):
    directory = tmp_path / "output_data"
    directory.mkdir()
    (directory / parquet_datado(QUANDO)).write_text("dummy")
    (directory / "foo.zip").write_text("dummy")

    with pytest.raises(FileNotFoundError, match=bruto_datado(QUANDO)):
        artefatos_da_publicacao(QUANDO, directory)


def test_publicacao_sem_parquet_do_dia_e_recusada(tmp_path):
    directory = tmp_path / "output_data"
    directory.mkdir()

    with pytest.raises(FileNotFoundError, match="Parquet de 2026-09-02"):
        artefatos_da_publicacao(QUANDO, directory)


def test_republicacao_declara_a_ausencia_do_bruto(tmp_path):
    """Republicar um Parquet existente é a exceção, e é explícita."""
    directory = tmp_path / "output_data"
    directory.mkdir()
    (directory / parquet_datado(QUANDO)).write_text("dummy")

    artefatos = artefatos_da_publicacao(QUANDO, directory, exigir_bruto=False)

    assert [Path(f).name for f in artefatos] == [parquet_datado(QUANDO)]


# --- O upload ---------------------------------------------------------------


def _publicar(files, **kwargs):
    return upload_files_to_archive(
        identifier="test-identifier",
        title="Test Title",
        description="Test Description",
        files=files,
        **kwargs,
    )


@pytest.mark.usefixtures("mock_env")
@patch("upload_to_archive.upload")
def test_upload_files_to_archive_success(mock_upload, publicacao):
    _publicar(artefatos_da_publicacao(QUANDO, publicacao))

    mock_upload.assert_called_once()
    call_kwargs = mock_upload.call_args.kwargs
    assert call_kwargs["identifier"] == "test-identifier"
    assert call_kwargs["access_key"] == "test_access_key"
    assert call_kwargs["secret_key"] == "test_secret_key"
    assert len(call_kwargs["files"]) == 2
    assert not any("ignore.txt" in file for file in call_kwargs["files"])


@patch("upload_to_archive.upload")
def test_upload_files_to_archive_dry_run_needs_no_credentials(
    mock_upload, publicacao, monkeypatch, capsys
):
    monkeypatch.delenv("IA_ACCESS_KEY", raising=False)
    monkeypatch.delenv("IA_SECRET_KEY", raising=False)

    _publicar(artefatos_da_publicacao(QUANDO, publicacao), dry_run=True)

    mock_upload.assert_not_called()
    assert "[Dry Run] Simulação de upload." in capsys.readouterr().out


@patch("upload_to_archive.upload")
def test_upload_files_to_archive_no_credentials_fails(
    mock_upload, publicacao, monkeypatch
):
    monkeypatch.delenv("IA_ACCESS_KEY", raising=False)
    monkeypatch.delenv("IA_SECRET_KEY", raising=False)

    with pytest.raises(RuntimeError, match="Credenciais do Internet Archive"):
        _publicar(artefatos_da_publicacao(QUANDO, publicacao))

    mock_upload.assert_not_called()


@pytest.mark.usefixtures("mock_env")
@patch("upload_to_archive.upload")
def test_upload_sem_arquivos_e_recusado(mock_upload):
    with pytest.raises(FileNotFoundError, match="Nenhum arquivo a publicar"):
        _publicar([])

    mock_upload.assert_not_called()


@pytest.mark.usefixtures("mock_env")
@patch("upload_to_archive.upload")
def test_upload_files_to_archive_exception_propagates(mock_upload, publicacao):
    mock_upload.side_effect = RuntimeError("Simulated upload error")

    with pytest.raises(RuntimeError, match="Simulated upload error"):
        _publicar(artefatos_da_publicacao(QUANDO, publicacao))

    mock_upload.assert_called_once()


@pytest.mark.usefixtures("mock_env")
@patch("upload_to_archive.upload")
def test_upload_omits_collection_by_default(mock_upload, publicacao):
    """Sem IA_COLLECTION, o item sobe sem coleção declarada.

    Declarar uma coleção sem privilégio de escrita faz o Archive recusar o
    upload inteiro, e não há como saber de fora se a conta tem esse privilégio.
    """
    _publicar(artefatos_da_publicacao(QUANDO, publicacao))

    metadata = mock_upload.call_args.kwargs["metadata"]
    assert "collection" not in metadata
    assert metadata["mediatype"] == "data"


@pytest.mark.usefixtures("mock_env")
@patch("upload_to_archive.upload")
def test_upload_uses_collection_from_the_environment(
    mock_upload, publicacao, monkeypatch
):
    monkeypatch.setenv("IA_COLLECTION", "opensource_data")

    _publicar(artefatos_da_publicacao(QUANDO, publicacao))

    assert mock_upload.call_args.kwargs["metadata"]["collection"] == "opensource_data"


@pytest.mark.usefixtures("mock_env")
@patch("upload_to_archive.upload")
def test_upload_pede_espera_ao_archive(mock_upload, publicacao):
    """O rate limit do Archive é global; a biblioteca já sabe esperar."""
    _publicar(artefatos_da_publicacao(QUANDO, publicacao))

    kwargs = mock_upload.call_args.kwargs
    assert kwargs["retries"] >= 1
    assert kwargs["retries_sleep"] >= 1


# --- O manifesto ------------------------------------------------------------


@pytest.mark.usefixtures("mock_env", "sem_manifesto")
@patch("upload_to_archive.upload")
def test_o_manifesto_aponta_para_o_retrato_publicado(mock_upload):
    manifesto = publicar_manifesto(QUANDO, item_do_ano(QUANDO))

    assert manifesto["data"] == "2026-09-02"
    assert manifesto["item"] == item_do_ano(QUANDO)
    assert manifesto["parquet_url"].endswith(parquet_datado(QUANDO))
    assert item_do_ano(QUANDO) in manifesto["parquet_url"]

    kwargs = mock_upload.call_args.kwargs
    assert kwargs["identifier"] == ITEM_PONTEIRO
    assert Path(kwargs["files"][0]).name == MANIFESTO


@pytest.mark.usefixtures("mock_env", "sem_manifesto")
@patch("upload_to_archive.upload")
def test_o_manifesto_aponta_para_onde_o_dado_foi_publicado(mock_upload):
    """Com --archive-item-identifier, o item real diverge do item do ano.

    Um ponteiro que promete um endereço onde nada subiu é pior que nenhum.
    """
    manifesto = publicar_manifesto(QUANDO, "item-experimental")

    assert manifesto["item"] == "item-experimental"
    assert "item-experimental" in manifesto["parquet_url"]
    assert "item-experimental" in manifesto["bruto_url"]
    assert item_do_ano(QUANDO) not in manifesto["parquet_url"]


@pytest.mark.usefixtures("mock_env", "sem_manifesto")
@patch("upload_to_archive.upload")
def test_o_manifesto_vive_fora_dos_itens_anuais(mock_upload):
    """Se morasse no item do ano, o consumidor precisaria saber o ano."""
    publicar_manifesto(QUANDO, item_do_ano(QUANDO))

    itens = {c.kwargs["identifier"] for c in mock_upload.call_args_list}
    assert itens == {ITEM_PONTEIRO}


@pytest.mark.usefixtures("mock_env")
@patch("upload_to_archive.upload")
def test_republicar_o_passado_nao_rebaixa_o_ponteiro(mock_upload, monkeypatch, capsys):
    """--data serve a republicação histórica; ela não pode apagar o presente."""
    _com_manifesto(monkeypatch, "2026-09-03")

    resultado = publicar_manifesto(date(2026, 8, 31), item_do_ano(QUANDO))

    assert resultado is None
    mock_upload.assert_not_called()
    assert "2026-09-03" in capsys.readouterr().out


@pytest.mark.usefixtures("mock_env")
@patch("upload_to_archive.upload")
def test_republicar_o_mesmo_dia_corrige_o_ponteiro(mock_upload, monkeypatch):
    """Empate sobrescreve: republicar hoje é corrigir hoje."""
    _com_manifesto(monkeypatch, QUANDO.isoformat())

    assert publicar_manifesto(QUANDO, item_do_ano(QUANDO)) is not None
    mock_upload.assert_called_once()


@pytest.mark.usefixtures("mock_env")
@patch("upload_to_archive.upload")
def test_o_ponteiro_avanca_para_o_retrato_mais_novo(mock_upload, monkeypatch):
    _com_manifesto(monkeypatch, "2026-09-01")

    manifesto = publicar_manifesto(QUANDO, item_do_ano(QUANDO))

    assert manifesto["data"] == QUANDO.isoformat()
    mock_upload.assert_called_once()


@pytest.mark.usefixtures("mock_env", "sem_manifesto")
@patch("upload_to_archive.upload")
def test_a_primeira_publicacao_cria_o_ponteiro(mock_upload):
    assert publicar_manifesto(QUANDO, item_do_ano(QUANDO)) is not None
    mock_upload.assert_called_once()


@patch("upload_to_archive.upload")
def test_o_manifesto_respeita_o_dry_run(mock_upload):
    publicar_manifesto(QUANDO, item_do_ano(QUANDO), dry_run=True)

    mock_upload.assert_not_called()
