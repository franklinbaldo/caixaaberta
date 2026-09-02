import argparse
import os
from pathlib import Path

from dotenv import load_dotenv
from internetarchive import upload

# O nome é parte do contrato de publicação, não um detalhe do empacotamento:
# é por ele que o gate abaixo reconhece a fonte primária, e é ele que
# run_pipeline gera. Um zip qualquer no diretório não substitui o bruto.
BRUTO_FILENAME = "imoveis_csv_bruto.zip"

# O Archive raciona uploads quando a fila GLOBAL dele se aproxima do teto, e
# recusa com "Please reduce your request rate - total_tasks_queued exceeds
# global_limit". O limite não é do item nem da conta: em 02/09/2026 a recusa
# veio com bucket_tasks_queued e accesskey_tasks_queued zerados, e
# total_tasks_queued em 11.639 de 11.999. Não há como evitá-lo publicando
# menos; só esperar. A biblioteca já sabe fazer isso.
#
# O estado atual pode ser consultado em
# https://s3.us.archive.org/?check_limit=1
UPLOAD_RETRIES = 5
UPLOAD_RETRIES_SLEEP = 30


def upload_files_to_archive(
    identifier, title, description, files_dir, dry_run=False, exigir_bruto=True
):
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

    # Publicar dado novo sem a fonte que o gerou quebra a proveniência: o
    # Parquet é derivado e some do rastro quem o originou. A exceção é a
    # republicação de um Parquet já existente, onde não há bruto novo a
    # preservar — e nesse caso quem chama declara isso com exigir_bruto=False.
    if exigir_bruto and not (Path(files_dir) / BRUTO_FILENAME).is_file():
        raise FileNotFoundError(
            f"{BRUTO_FILENAME} não encontrado em {files_dir}. Toda publicação "
            "de dado novo leva a fonte junto; para republicar um Parquet "
            "existente, chame com exigir_bruto=False."
        )

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
        retries=UPLOAD_RETRIES,
        retries_sleep=UPLOAD_RETRIES_SLEEP,
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
