from pathlib import Path
from unittest.mock import patch

import pytest

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from upload_to_archive import upload_files_to_archive


@pytest.fixture
def mock_env(monkeypatch):
    monkeypatch.setenv("IA_ACCESS_KEY", "test_access_key")
    monkeypatch.setenv("IA_SECRET_KEY", "test_secret_key")


@pytest.fixture
def dummy_files_dir(tmp_path):
    directory = tmp_path / "output_data"
    directory.mkdir()
    (directory / "imoveis_1.parquet").write_text("dummy")
    (directory / "imoveis_2.parquet").write_text("dummy")
    (directory / "imoveis_csv_bruto.zip").write_text("dummy")
    (directory / "ignore.txt").write_text("ignore")
    return directory


@pytest.mark.usefixtures("mock_env")
@patch("upload_to_archive.upload")
def test_upload_files_to_archive_success(mock_upload, dummy_files_dir):
    upload_files_to_archive(
        identifier="test-identifier",
        title="Test Title",
        description="Test Description",
        files_dir=str(dummy_files_dir),
    )

    mock_upload.assert_called_once()
    call_kwargs = mock_upload.call_args.kwargs
    assert call_kwargs["identifier"] == "test-identifier"
    assert call_kwargs["access_key"] == "test_access_key"
    assert call_kwargs["secret_key"] == "test_secret_key"
    # dois Parquets e o zip com o CSV bruto; o .txt fica de fora
    assert len(call_kwargs["files"]) == 3
    assert not any("ignore.txt" in file for file in call_kwargs["files"])


@patch("upload_to_archive.upload")
def test_upload_files_to_archive_dry_run_needs_no_credentials(
    mock_upload, dummy_files_dir, monkeypatch, capsys
):
    monkeypatch.delenv("IA_ACCESS_KEY", raising=False)
    monkeypatch.delenv("IA_SECRET_KEY", raising=False)

    upload_files_to_archive(
        identifier="test-identifier",
        title="Test Title",
        description="Test Description",
        files_dir=str(dummy_files_dir),
        dry_run=True,
    )

    mock_upload.assert_not_called()
    assert "[Dry Run] Simulação de upload." in capsys.readouterr().out


@patch("upload_to_archive.upload")
def test_upload_files_to_archive_no_credentials_fails(
    mock_upload, dummy_files_dir, monkeypatch
):
    monkeypatch.delenv("IA_ACCESS_KEY", raising=False)
    monkeypatch.delenv("IA_SECRET_KEY", raising=False)

    with pytest.raises(RuntimeError, match="Credenciais do Internet Archive"):
        upload_files_to_archive(
            identifier="test",
            title="t",
            description="d",
            files_dir=str(dummy_files_dir),
        )

    mock_upload.assert_not_called()


@pytest.mark.usefixtures("mock_env")
@patch("upload_to_archive.upload")
def test_upload_files_to_archive_no_parquet_files_fails(mock_upload, tmp_path):
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()

    with pytest.raises(FileNotFoundError, match="Nenhum arquivo .parquet"):
        upload_files_to_archive(
            identifier="test",
            title="t",
            description="d",
            files_dir=str(empty_dir),
        )

    mock_upload.assert_not_called()


@pytest.mark.usefixtures("mock_env")
@patch("upload_to_archive.upload")
def test_upload_files_to_archive_exception_propagates(
    mock_upload, dummy_files_dir
):
    mock_upload.side_effect = RuntimeError("Simulated upload error")

    with pytest.raises(RuntimeError, match="Simulated upload error"):
        upload_files_to_archive(
            identifier="test",
            title="t",
            description="d",
            files_dir=str(dummy_files_dir),
        )

    mock_upload.assert_called_once()


@pytest.mark.usefixtures("mock_env")
@patch("upload_to_archive.upload")
def test_upload_omits_collection_by_default(mock_upload, dummy_files_dir):
    """Sem IA_COLLECTION, o item sobe sem coleção declarada.

    Declarar uma coleção sem privilégio de escrita faz o Archive recusar o
    upload inteiro, e não há como saber de fora se a conta tem esse privilégio.
    """
    upload_files_to_archive(
        identifier="test-identifier",
        title="Test Title",
        description="Test Description",
        files_dir=str(dummy_files_dir),
    )

    metadata = mock_upload.call_args.kwargs["metadata"]
    assert "collection" not in metadata
    assert metadata["mediatype"] == "data"


@pytest.mark.usefixtures("mock_env")
@patch("upload_to_archive.upload")
def test_upload_uses_collection_from_the_environment(
    mock_upload, dummy_files_dir, monkeypatch
):
    monkeypatch.setenv("IA_COLLECTION", "opensource_data")

    upload_files_to_archive(
        identifier="test-identifier",
        title="Test Title",
        description="Test Description",
        files_dir=str(dummy_files_dir),
    )

    assert mock_upload.call_args.kwargs["metadata"]["collection"] == "opensource_data"


@pytest.mark.usefixtures("mock_env")
@patch("upload_to_archive.upload")
def test_publicacao_sem_o_bruto_e_recusada(mock_upload, tmp_path):
    """Dado novo sem a fonte que o gerou quebra a proveniência."""
    directory = tmp_path / "output_data"
    directory.mkdir()
    (directory / "imoveis_geocoded.parquet").write_text("dummy")

    with pytest.raises(FileNotFoundError, match="CSV bruto"):
        upload_files_to_archive(
            identifier="test-identifier",
            title="Test Title",
            description="Test Description",
            files_dir=str(directory),
        )

    mock_upload.assert_not_called()


@pytest.mark.usefixtures("mock_env")
@patch("upload_to_archive.upload")
def test_republicacao_declara_a_ausencia_do_bruto(mock_upload, tmp_path):
    """Republicar um Parquet existente é a exceção, e é explícita."""
    directory = tmp_path / "output_data"
    directory.mkdir()
    (directory / "imoveis_geocoded.parquet").write_text("dummy")

    upload_files_to_archive(
        identifier="test-identifier",
        title="Test Title",
        description="Test Description",
        files_dir=str(directory),
        exigir_bruto=False,
    )

    mock_upload.assert_called_once()


@pytest.mark.usefixtures("mock_env")
@patch("upload_to_archive.upload")
def test_o_bruto_sobe_junto_do_parquet(mock_upload, dummy_files_dir):
    upload_files_to_archive(
        identifier="test-identifier",
        title="Test Title",
        description="Test Description",
        files_dir=str(dummy_files_dir),
    )

    enviados = mock_upload.call_args.kwargs["files"]
    assert any(f.endswith("imoveis_csv_bruto.zip") for f in enviados)
    assert not any(f.endswith(".txt") for f in enviados)
