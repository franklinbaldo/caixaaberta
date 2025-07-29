import argparse
import os
from pathlib import Path
from dotenv import load_dotenv
from internetarchive import upload

def upload_files_to_archive(identifier, title, description, files_dir, dry_run=False):
    """
    Uploads all files in a directory to a specified Internet Archive item.

    Args:
        identifier (str): The Internet Archive item identifier.
        title (str): The title of the item.
        description (str): The description of the item.
        files_dir (str): The directory containing the files to upload.
        dry_run (bool): If True, simulates the upload without making changes.
    """
    load_dotenv()
    access_key = os.getenv("IA_ACCESS_KEY")
    secret_key = os.getenv("IA_SECRET_KEY")

    if not access_key or not secret_key:
        print("Credenciais do Internet Archive não encontradas. Defina IA_ACCESS_KEY e IA_SECRET_KEY em seu arquivo .env.")
        return

    files_to_upload = [str(p) for p in Path(files_dir).glob("*.parquet")]
    if not files_to_upload:
        print(f"Nenhum arquivo .parquet encontrado em {files_dir}")
        return

    metadata = {
        "title": title,
        "description": description,
        "mediatype": "data",
        "collection": "opensource_data",
        "subject": ["real estate", "brazil", "caixa"],
    }

    print(f"Iniciando o upload para o item: {identifier}")
    if dry_run:
        print("[Dry Run] Simulação de upload. Nenhum arquivo será enviado.")
        print(f"Identifier: {identifier}")
        print(f"Metadata: {metadata}")
        print(f"Files to upload: {files_to_upload}")
    else:
        try:
            upload(
                identifier=identifier,
                files=files_to_upload,
                metadata=metadata,
                access_key=access_key,
                secret_key=secret_key,
                verbose=True,
            )
            print("Upload concluído com sucesso.")
        except Exception as e:
            print(f"Ocorreu um erro durante o upload: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload de arquivos para o Internet Archive.")
    parser.add_argument("--identifier", required=True, help="O identificador do item no Internet Archive.")
    parser.add_argument("--title", required=True, help="O título do item.")
    parser.add_argument("--description", required=True, help="A descrição do item.")
    parser.add_argument("--files_dir", default="output_data", help="O diretório que contém os arquivos a serem carregados.")
    parser.add_argument("--dry-run", action="store_true", help="Simula o upload sem enviar arquivos.")
    args = parser.parse_args()

    upload_files_to_archive(args.identifier, args.title, args.description, args.files_dir, args.dry_run)
