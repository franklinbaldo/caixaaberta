from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from fetch_data import parse_caixa_csv


CAIXA_CSV = (
    "Lista de Imóveis da Caixa;;Data de geração:;31/08/2026;;;;;;;\n"
    "N° do imóvel;UF;Cidade;Bairro;Endereço;Preço;Valor de avaliação;Desconto;"
    "Financiamento;Descrição;Modalidade de venda;Link de acesso\n"
    "10218509;RO;PORTO VELHO;CENTRO;RUA A, 10;99.743,11;170.000,00;41.33;"
    "Não;Casa.;Venda Direta Online;https://venda-imoveis.caixa.gov.br/oferta/10218509\n"
)


def test_link_de_acesso_da_caixa_chega_ao_dataset_normalizado():
    frame = parse_caixa_csv(CAIXA_CSV.encode("latin-1"))

    assert "link_acesso" in frame.columns
    assert frame.iloc[0]["link_acesso"] == (
        "https://venda-imoveis.caixa.gov.br/oferta/10218509"
    )
