import pytest
import os
import sys
from unittest.mock import patch, MagicMock
from pathlib import Path

# Add src to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from upload_to_archive import upload_files_to_archive

@pytest.fixture
def mock_env(monkeypatch):
    monkeypatch.setenv("IA_ACCESS_KEY", "test_access_key")
    monkeypatch.setenv("IA_SECRET_KEY", "test_secret_key")

@pytest.fixture
def dummy_files_dir(tmp_path):
    d = tmp_path / "output_data"
    d.mkdir()
    (d / "imoveis_1.parquet").write_text("dummy")
    (d / "imoveis_2.parquet").write_text("dummy")
    # Add a non-parquet file to test filtering
    (d / "ignore.txt").write_text("ignore")
    return d

@patch("upload_to_archive.upload")
def test_upload_files_to_archive_success(mock_upload, mock_env, dummy_files_dir):
    upload_files_to_archive(
        identifier="test-identifier",
        title="Test Title",
        description="Test Description",
        files_dir=str(dummy_files_dir),
        dry_run=False
    )

    mock_upload.assert_called_once()

    # Check arguments
    call_kwargs = mock_upload.call_args.kwargs
    assert call_kwargs["identifier"] == "test-identifier"
    assert call_kwargs["access_key"] == "test_access_key"
    assert call_kwargs["secret_key"] == "test_secret_key"

    # Check that only .parquet files were included
    uploaded_files = call_kwargs["files"]
    assert len(uploaded_files) == 2
    assert any("imoveis_1.parquet" in f for f in uploaded_files)
    assert any("imoveis_2.parquet" in f for f in uploaded_files)
    assert not any("ignore.txt" in f for f in uploaded_files)

    # Check metadata
    metadata = call_kwargs["metadata"]
    assert metadata["title"] == "Test Title"
    assert metadata["description"] == "Test Description"
    assert metadata["mediatype"] == "data"

@patch("upload_to_archive.upload")
def test_upload_files_to_archive_dry_run(mock_upload, mock_env, dummy_files_dir, capsys):
    upload_files_to_archive(
        identifier="test-identifier",
        title="Test Title",
        description="Test Description",
        files_dir=str(dummy_files_dir),
        dry_run=True
    )

    mock_upload.assert_not_called()
    captured = capsys.readouterr()
    assert "[Dry Run] Simulação de upload." in captured.out
    assert "test-identifier" in captured.out
    assert "imoveis_1.parquet" in captured.out

@patch("upload_to_archive.upload")
def test_upload_files_to_archive_no_credentials(mock_upload, dummy_files_dir, monkeypatch, capsys):
    # Ensure no credentials are set
    monkeypatch.delenv("IA_ACCESS_KEY", raising=False)
    monkeypatch.delenv("IA_SECRET_KEY", raising=False)

    upload_files_to_archive(
        identifier="test", title="t", description="d", files_dir=str(dummy_files_dir)
    )

    mock_upload.assert_not_called()
    captured = capsys.readouterr()
    assert "Credenciais do Internet Archive não encontradas" in captured.out

@patch("upload_to_archive.upload")
def test_upload_files_to_archive_no_parquet_files(mock_upload, mock_env, tmp_path, capsys):
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()

    upload_files_to_archive(
        identifier="test", title="t", description="d", files_dir=str(empty_dir)
    )

    mock_upload.assert_not_called()
    captured = capsys.readouterr()
    assert "Nenhum arquivo .parquet encontrado" in captured.out

@patch("upload_to_archive.upload")
def test_upload_files_to_archive_exception(mock_upload, mock_env, dummy_files_dir, capsys):
    mock_upload.side_effect = Exception("Simulated upload error")

    upload_files_to_archive(
        identifier="test", title="t", description="d", files_dir=str(dummy_files_dir)
    )

    mock_upload.assert_called_once()
    captured = capsys.readouterr()
    assert "Ocorreu um erro durante o upload: Simulated upload error" in captured.out
