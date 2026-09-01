import argparse
import os
from pathlib import Path

from dotenv import load_dotenv
from internetarchive import upload


def upload_files_to_archive(identifier, title, description, files_dir, dry_run=False):
    """Upload all Parquet files in ``files_dir`` to one Internet Archive item.

    Publication is part of the pipeline contract: when a real upload is requested,
    missing credentials or an upload error must propagate so CI cannot report a
    false green. Dry-run remains available without credentials.
    """
    load_dotenv()

    # O Parquet é o dado publicado; o zip preserva o CSV como a Caixa serviu.
    publicaveis = ("*.parquet", "*.zip")
    files_to_upload = sorted(
        str(caminho)
        for padrao in publicaveis
        for caminho in Path(files_dir).glob(padrao)
    )
    if not any(f.endswith(".parquet") for f in files_to_upload):
        raise FileNotFoundError(f"Nenhum arquivo .parquet encontrado em {files_dir}")

    metadata = {
        "title": title,
        "description": description,
        "mediatype": "data",
        "subject": ["real estate", "brazil", "caixa"],
    }

    # A coleção é opcional de propósito. Declarar uma em que a conta não tem
    # privilégio de escrita faz o Archive recusar o upload inteiro com
    # "Access Denied - You lack sufficient privileges to write to those
    # collections" — foi o que aconteceu com "opensource_data". Sem coleção, o
    # item entra na área geral da conta e um curador pode movê-lo depois.
    collection = os.getenv("IA_COLLECTION")
    if collection:
        metadata["collection"] = collection

    print(f"Iniciando o upload para o item: {identifier}")
    if dry_run:
        print("[Dry Run] Simulação de upload. Nenhum arquivo será enviado.")
        print(f"Identifier: {identifier}")
        print(f"Metadata: {metadata}")
        print(f"Files to upload: {files_to_upload}")
        return

    access_key = os.getenv("IA_ACCESS_KEY")
    secret_key = os.getenv("IA_SECRET_KEY")
    if not access_key or not secret_key:
        raise RuntimeError(
            "Credenciais do Internet Archive não encontradas. "
            "Defina IA_ACCESS_KEY e IA_SECRET_KEY antes de publicar."
        )

    upload(
        identifier=identifier,
        files=files_to_upload,
        metadata=metadata,
        access_key=access_key,
        secret_key=secret_key,
        verbose=True,
    )
    print("Upload concluído com sucesso.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload de arquivos para o Internet Archive.")
    parser.add_argument("--identifier", required=True, help="O identificador do item no Internet Archive.")
    parser.add_argument("--title", required=True, help="O título do item.")
    parser.add_argument("--description", required=True, help="A descrição do item.")
    parser.add_argument(
        "--files_dir",
        default="output_data",
        help="O diretório que contém os arquivos a serem carregados.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Simula o upload sem enviar arquivos.")
    args = parser.parse_args()

    upload_files_to_archive(
        args.identifier,
        args.title,
        args.description,
        args.files_dir,
        args.dry_run,
    )
