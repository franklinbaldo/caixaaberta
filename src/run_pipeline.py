import argparse
import shutil
import tempfile
from datetime import date
from pathlib import Path

from archive_names import bruto_datado, data_de_publicacao, item_do_ano
from fetch_data import fetch_all_states, process_local_data
from reporter import parquet_do_dia, validate_publication_parquet
from upload_to_archive import (
    artefatos_da_publicacao,
    publicar_manifesto,
    upload_files_to_archive,
)


def main():
    parser = argparse.ArgumentParser(
        description="Pipeline de dados imobiliários: processa dados locais e faz upload para o Internet Archive."
    )
    parser.add_argument(
        "--skip-fetch",
        action="store_true",
        help=(
            "Pula o download dos dados da Caixa e usa os CSVs já presentes em "
            "data/. Implícito em --skip-processing."
        ),
    )
    parser.add_argument(
        "--skip-processing",
        action="store_true",
        help="Pula a etapa de processamento de dados locais.",
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Pula a etapa de upload para o Internet Archive.",
    )
    parser.add_argument(
        "--upload-dry-run",
        action="store_true",
        help="Simula o upload para o Internet Archive.",
    )
    parser.add_argument(
        "--data",
        type=date.fromisoformat,
        help=(
            "Data desta publicação (AAAA-MM-DD). Sem isso, hoje em UTC. "
            "Serve a republicações e à reprodutibilidade."
        ),
    )
    parser.add_argument(
        "--archive-item-identifier",
        help=(
            "O identificador do item no Internet Archive. Sem isso, o item do "
            "ano da publicação."
        ),
    )
    parser.add_argument(
        "--archive-item-title",
        default="Imóveis da Caixa Econômica Federal",
        help="O título do item no Internet Archive.",
    )
    parser.add_argument(
        "--archive-item-description",
        default=(
            "Dados de imóveis da Caixa Econômica Federal, processados e "
            "disponibilizados em formato Parquet."
        ),
        help="A descrição do item no Internet Archive.",
    )
    args = parser.parse_args()

    # A data é propriedade da execução, lida uma vez. Se cada etapa
    # consultasse o relógio, uma execução atravessando a meia-noite gravaria o
    # zip num dia e o Parquet no outro — e, na virada do ano, mandaria o
    # arquivo de 1º de janeiro para o item do ano anterior.
    quando = args.data or data_de_publicacao()
    identifier = args.archive_item_identifier or item_do_ano(quando)
    print(f"Publicação de {quando.isoformat()} no item {identifier}.")

    # O download só serve ao processamento: baixar CSVs para em seguida
    # ignorá-los faria --skip-processing depender da Caixa estar acessível.
    if args.skip_processing or args.skip_fetch:
        print("Pulando o download dos dados da Caixa.")
    else:
        print("Baixando os dados da Caixa...")
        with tempfile.TemporaryDirectory() as bruto:
            fetch_all_states(raw_dir=bruto)
            # O CSV como a Caixa serviu vai para o Archive junto do Parquet: é
            # a fonte primária, e some assim que a Caixa atualiza a lista.
            saida = Path("output_data")
            saida.mkdir(parents=True, exist_ok=True)
            datado = saida / bruto_datado(quando)
            shutil.make_archive(str(datado.with_suffix("")), "zip", root_dir=bruto)
        print("Download dos dados da Caixa concluído.")

    if not args.skip_processing:
        print("Iniciando o processamento de dados locais...")
        process_local_data(quando=quando)
        print("Processamento de dados locais concluído.")
    else:
        print("Pulando o processamento de dados locais.")

    if args.skip_upload:
        print("Pulando o upload para o Internet Archive.")
        return

    print("Validando o Parquet antes da publicação...")
    validate_publication_parquet(parquet_do_dia(quando))
    print("Parquet validado para publicação.")

    print("Iniciando o upload para o Internet Archive...")
    upload_files_to_archive(
        identifier=identifier,
        title=args.archive_item_title,
        description=args.archive_item_description,
        files=artefatos_da_publicacao(
            quando,
            "output_data",
            # --skip-processing republica o Parquet que já existe; não há bruto
            # novo a preservar, e é a única publicação sem a fonte junto.
            exigir_bruto=not args.skip_processing,
        ),
        dry_run=args.upload_dry_run,
    )

    # Só depois do upload: o ponteiro promete um arquivo que existe. E só para
    # a série pública: publicar num item arbitrário é um experimento, e um
    # experimento não redireciona quem consulta o dataset.
    if identifier == item_do_ano(quando):
        publicar_manifesto(quando, identifier, dry_run=args.upload_dry_run)
    else:
        print(
            f"Item {identifier} não é o da série ({item_do_ano(quando)}); "
            "o ponteiro do último retrato fica onde está."
        )

    if args.upload_dry_run:
        print("Dry-run de upload concluído.")
    else:
        print("Publicação no Internet Archive concluída.")


if __name__ == "__main__":
    main()
