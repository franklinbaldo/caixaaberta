import argparse
import json
import os
import tempfile
from datetime import date
from pathlib import Path

import requests
from dotenv import load_dotenv
from internetarchive import upload

from archive_names import (
    ITEM_PONTEIRO,
    MANIFESTO,
    data_de_publicacao,
    bruto_datado,
    parquet_datado,
    url_do_manifesto,
    url_no_item,
)

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


def _credenciais() -> tuple[str, str]:
    load_dotenv()
    access_key = os.getenv("IA_ACCESS_KEY")
    secret_key = os.getenv("IA_SECRET_KEY")
    if not access_key or not secret_key:
        raise RuntimeError(
            "Credenciais do Internet Archive não encontradas. "
            "Defina IA_ACCESS_KEY e IA_SECRET_KEY antes de publicar."
        )
    return access_key, secret_key


def artefatos_da_publicacao(
    quando: date, files_dir, exigir_bruto: bool = True
) -> list[str]:
    """Os arquivos daquela publicação — por nome exato, nunca por glob.

    Varrer o diretório publicaria o que estivesse ali: o Parquet de hoje ao
    lado do bruto de ontem passaria pelo gate de proveniência, e na virada do
    ano retratos velhos subiriam para o item novo. O contrato é um par
    ``(data, parquet, bruto)``, e é assim que ele é montado.
    """
    diretorio = Path(files_dir)
    parquet = diretorio / parquet_datado(quando)
    if not parquet.exists():
        raise FileNotFoundError(
            f"Parquet de {quando.isoformat()} não encontrado: {parquet}"
        )

    artefatos = [str(parquet)]

    # Publicar dado novo sem a fonte que o gerou quebra a proveniência: o
    # Parquet é derivado e some do rastro quem o originou. A exceção é a
    # republicação de um Parquet já existente, onde não há bruto novo a
    # preservar — e nesse caso quem chama declara isso com exigir_bruto=False.
    bruto = diretorio / bruto_datado(quando)
    if bruto.exists():
        artefatos.append(str(bruto))
    elif exigir_bruto:
        raise FileNotFoundError(
            f"{bruto} não encontrado. Toda publicação de dado novo leva a "
            "fonte do mesmo dia junto; para republicar um Parquet existente, "
            "chame com exigir_bruto=False."
        )

    return artefatos


def manifesto_publicado() -> dict | None:
    """Lê o ponteiro atual do Archive. ``None`` se ainda não existe."""
    try:
        resposta = requests.get(url_do_manifesto(), timeout=30)
    except requests.RequestException as exc:
        raise RuntimeError(
            f"Não foi possível ler o manifesto em {url_do_manifesto()}: {exc}. "
            "Publicar sem saber onde o ponteiro está poderia fazê-lo andar "
            "para trás."
        ) from exc

    if resposta.status_code == 404:
        return None
    resposta.raise_for_status()
    return resposta.json()


def publicar_manifesto(
    quando: date, identifier: str, dry_run: bool = False
) -> dict | None:
    """Aponta o item-ponteiro para o retrato recém-publicado.

    O calendário não serve como ponteiro: se a publicação do dia falhar, "hoje"
    aponta para um arquivo que não existe. Este manifesto é o único nome
    sobrescrito a cada publicação, e não guarda dado — só o endereço do último
    retrato que de fato subiu.

    O ponteiro é **monotônico**: republicar um retrato histórico com ``--data``
    não rebaixa o "mais recente". Maior data publicada vence; empate sobrescreve,
    porque republicar o mesmo dia é corrigir aquele dia.

    ``identifier`` é o item onde os dados de fato foram publicados — não o item
    do ano derivado da data. Com ``--archive-item-identifier``, os dois divergem,
    e o manifesto que promete um endereço onde nada subiu é pior que nenhum.
    """
    manifesto = {
        "data": quando.isoformat(),
        "item": identifier,
        "parquet_url": url_no_item(identifier, parquet_datado(quando)),
        "bruto_url": url_no_item(identifier, bruto_datado(quando)),
    }

    if dry_run:
        print(f"[Dry Run] Manifesto que seria publicado: {manifesto}")
        return manifesto

    atual = manifesto_publicado()
    if atual and atual.get("data", "") > manifesto["data"]:
        print(
            f"Manifesto preservado: o ponteiro já está em {atual['data']}, "
            f"depois de {manifesto['data']}. Republicação não rebaixa o "
            "último retrato."
        )
        return None

    access_key, secret_key = _credenciais()
    with tempfile.TemporaryDirectory() as tmp:
        caminho = Path(tmp) / MANIFESTO
        caminho.write_text(json.dumps(manifesto, indent=2), encoding="utf-8")
        upload(
            identifier=ITEM_PONTEIRO,
            files=[str(caminho)],
            metadata={
                "title": "Imóveis da Caixa Econômica Federal — último retrato",
                "description": (
                    "Ponteiro para o retrato mais recente publicado. Os dados "
                    "vivem nos itens anuais."
                ),
                "mediatype": "data",
            },
            access_key=access_key,
            secret_key=secret_key,
            verbose=True,
            retries=UPLOAD_RETRIES,
            retries_sleep=UPLOAD_RETRIES_SLEEP,
        )
    print(f"Manifesto atualizado: {manifesto['parquet_url']}")
    return manifesto


def upload_files_to_archive(identifier, title, description, files, dry_run=False):
    """Publica os arquivos de uma publicação em um item do Internet Archive.

    Recebe a lista exata de arquivos — montada por ``artefatos_da_publicacao``,
    que garante que todos são do mesmo dia. Publication is part of the pipeline
    contract: when a real upload is requested, missing credentials or an upload
    error must propagate so CI cannot report a false green.
    """
    load_dotenv()

    files_to_upload = sorted(str(caminho) for caminho in files)
    if not files_to_upload:
        raise FileNotFoundError("Nenhum arquivo a publicar.")

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

    access_key, secret_key = _credenciais()

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
    parser = argparse.ArgumentParser(
        description="Upload de arquivos para o Internet Archive."
    )
    parser.add_argument(
        "--identifier",
        required=True,
        help="O identificador do item no Internet Archive.",
    )
    parser.add_argument("--title", required=True, help="O título do item.")
    parser.add_argument("--description", required=True, help="A descrição do item.")
    parser.add_argument(
        "--files_dir",
        default="output_data",
        help="O diretório que contém os arquivos a serem carregados.",
    )
    parser.add_argument(
        "--data",
        type=date.fromisoformat,
        help="Data da publicação (AAAA-MM-DD). Sem isso, hoje em UTC.",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Simula o upload sem enviar arquivos."
    )
    args = parser.parse_args()

    quando = args.data or data_de_publicacao()
    upload_files_to_archive(
        args.identifier,
        args.title,
        args.description,
        artefatos_da_publicacao(quando, args.files_dir),
        args.dry_run,
    )
    publicar_manifesto(quando, args.identifier, dry_run=args.dry_run)
