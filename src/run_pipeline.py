import argparse
from fetch_data import process_local_data
from upload_to_archive import upload_files_to_archive

def main():
    parser = argparse.ArgumentParser(description="Pipeline de dados imobiliários: processa dados locais e faz upload para o Internet Archive.")
    parser.add_argument("--skip-processing", action="store_true", help="Pula a etapa de processamento de dados locais.")
    parser.add_argument("--skip-upload", action="store_true", help="Pula a etapa de upload para o Internet Archive.")
    parser.add_argument("--upload-dry-run", action="store_true", help="Simula o upload para o Internet Archive.")
    parser.add_argument("--archive-item-identifier", default="imoveis-caixa-economica-federal", help="O identificador do item no Internet Archive.")
    parser.add_argument("--archive-item-title", default="Imóveis da Caixa Econômica Federal", help="O título do item no Internet Archive.")
    parser.add_argument("--archive-item-description", default="Dados de imóveis da Caixa Econômica Federal, processados e disponibilizados em formato Parquet.", help="A descrição do item no Internet Archive.")
    parser.add_argument("--files-dir", default="output_data", help="O diretório que contém os arquivos a serem carregados.")

    args = parser.parse_args()

    if not args.skip_processing:
        print("Iniciando o processamento de dados locais...")
        process_local_data()
        print("Processamento de dados locais concluído.")
    else:
        print("Pulando o processamento de dados locais.")

    if not args.skip_upload:
        print("Iniciando o upload para o Internet Archive...")
        upload_files_to_archive(
            identifier=args.archive_item_identifier,
            title=args.archive_item_title,
            description=args.archive_item_description,
            files_dir=args.files_dir,
            dry_run=args.upload_dry_run,
        )
        print("Upload para o Internet Archive concluído.")
    else:
        print("Pulando o upload para o Internet Archive.")

if __name__ == "__main__":
    main()
