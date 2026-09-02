import argparse
import shutil
import tempfile
from pathlib import Path

from fetch_data import fetch_all_states, process_local_data
from reporter import DEFAULT_PARQUET_PATH, validate_publication_parquet
from archive_names import BRUTO_LATEST, bruto_datado, item_do_ano
from upload_to_archive import upload_files_to_archive


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
        "--archive-item-identifier",
        default=item_do_ano(),
        help="O identificador do item no Internet Archive.",
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
            # Um zip datado, que fica, e uma cópia de nome estável, que
            # o consumo corrente encontra sem saber a data de hoje.
            datado = saida / bruto_datado()
            shutil.make_archive(str(datado.with_suffix("")), "zip", root_dir=bruto)
            shutil.copy2(datado, saida / BRUTO_LATEST)
        print("Download dos dados da Caixa concluído.")

    if not args.skip_processing:
        print("Iniciando o processamento de dados locais...")
        process_local_data()
        print("Processamento de dados locais concluído.")
    else:
        print("Pulando o processamento de dados locais.")

    if args.skip_upload:
        print("Pulando o upload para o Internet Archive.")
        return

    print("Validando o Parquet antes da publicação...")
    validate_publication_parquet(DEFAULT_PARQUET_PATH)
    print("Parquet validado para publicação.")

    print("Iniciando o upload para o Internet Archive...")
    upload_files_to_archive(
        identifier=args.archive_item_identifier,
        title=args.archive_item_title,
        description=args.archive_item_description,
        files_dir="output_data",
        dry_run=args.upload_dry_run,
        # --skip-processing republica o Parquet que já existe; não há bruto
        # novo a preservar, e é a única publicação sem a fonte junto.
        exigir_bruto=not args.skip_processing,
    )
    if args.upload_dry_run:
        print("Dry-run de upload concluído.")
    else:
        print("Publicação no Internet Archive concluída.")


if __name__ == "__main__":
    main()
