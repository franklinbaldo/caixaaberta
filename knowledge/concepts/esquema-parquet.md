---
type: Schema
title: Esquema do Parquet
description: Colunas do arquivo publicado e o que cada uma significa
artifact: output_data/imoveis_geocoded.parquet
---

# Esquema do Parquet

| Coluna          | Tipo  | Significado                                                                                                                                                                                                                                            |
| --------------- | ----- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `link`          | texto | Número do imóvel na Caixa. Chave prática; a validação de publicação exige que ao menos um valor não seja vazio.                                                                                                                                        |
| `endereco`      | texto | Logradouro, número e complemento, como a Caixa escreve.                                                                                                                                                                                                |
| `bairro`        | texto | Normalizado para maiúsculas, sem espaços nas pontas.                                                                                                                                                                                                   |
| `cidade`        | texto | Município.                                                                                                                                                                                                                                             |
| `estado`        | texto | Sigla da UF.                                                                                                                                                                                                                                           |
| `descricao`     | texto | Tipo do imóvel e áreas, em texto corrido.                                                                                                                                                                                                              |
| `preco`         | float | Preço de venda ou lance mínimo, conforme a modalidade — ver [preço não é preço de venda](armadilhas/preco-nao-e-preco-de-venda.md).                                                                                                                    |
| `avaliacao`     | float | Valor de avaliação em reais.                                                                                                                                                                                                                           |
| `desconto`      | float | Percentual sobre a avaliação. Zero é valor legítimo, não ausência — ver [desconto zero](armadilhas/desconto-zero-nao-e-ausencia-de-dado.md).                                                                                                           |
| `financiamento` | texto | `Sim` ou `Não`.                                                                                                                                                                                                                                        |
| `modalidade`    | texto | Uma das quatro formas de venda: [Venda Direta Online](modalidades/venda-direta-online.md), [Venda Online](modalidades/venda-online.md), [Leilão SFI - Edital Único](modalidades/leilao-sfi.md) ou [Licitação Aberta](modalidades/licitacao-aberta.md). |
| `foto`          | texto | Vazio. Coluna herdada de versões anteriores.                                                                                                                                                                                                           |
| `latitude`      | float | Nulo quando a [geocodificação](geocodificacao.md) não resolve.                                                                                                                                                                                         |
| `longitude`     | float | Idem.                                                                                                                                                                                                                                                  |

`preco`, `avaliacao` e `desconto` chegam da Caixa em formato brasileiro
(`99.743,11`) e são convertidos para float no download.
