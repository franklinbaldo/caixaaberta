# Conhecimento do Caixa Aberta

Bundle [OKF v0.2](https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md)
que descreve o dataset publicado por este repositório: de onde o dado vem, o
que o pipeline faz com ele, o que cada coluna significa e onde o resultado é
publicado.

O bundle existe para quem consome o Parquet no Internet Archive sem ler o
código. É validado no CI com `okf-parser check knowledge`.

- [Fonte: listas da Caixa](concepts/fonte-caixa.md)
- [Pipeline de consolidação](concepts/pipeline.md)
- [Geocodificação](concepts/geocodificacao.md)
- [Dataset de imóveis](concepts/dataset-imoveis.md)
- [Esquema do Parquet](concepts/esquema-parquet.md)
- [Publicação no Internet Archive](concepts/publicacao-archive.md)

## Modalidades de venda

O campo que mais muda a leitura de um imóvel. Só a primeira tem preço fixo.

- [Venda Direta Online](concepts/modalidades/venda-direta-online.md)
- [Venda Online](concepts/modalidades/venda-online.md)
- [Leilão SFI - Edital Único](concepts/modalidades/leilao-sfi.md)
- [Licitação Aberta](concepts/modalidades/licitacao-aberta.md)

## Armadilhas

Leituras intuitivas do dado que produzem conclusão errada.

- [preco não é o preço de venda](concepts/armadilhas/preco-nao-e-preco-de-venda.md)
- [desconto zero não é ausência de dado](concepts/armadilhas/desconto-zero-nao-e-ausencia-de-dado.md)
- [o acervo não tem histórico](concepts/armadilhas/acervo-nao-tem-historico.md)
- [geocodificado não é localizado](concepts/armadilhas/geocodificado-nao-e-localizado.md)

## Consultas

- [Acervo por modalidade e estado](concepts/consultas/acervo-por-modalidade.md)
- [Maiores descontos por estado](concepts/consultas/maiores-descontos-por-uf.md)

## Tipos

Cada `type` usado acima é definido em [`types/`](types/spec.md), e o CI recusa
um tipo novo sem definição.
