---
type: Schema
title: Esquema do Parquet
description: Colunas do arquivo publicado e o que cada uma significa
artifact: output_data/imoveis_geocoded.parquet
---

# Esquema do Parquet

| Coluna          | Tipo  | Significado                                                                                                     |
| --------------- | ----- | --------------------------------------------------------------------------------------------------------------- |
| `link`          | texto | Número do imóvel na Caixa. Chave prática; a validação de publicação exige que ao menos um valor não seja vazio. |
| `endereco`      | texto | Logradouro, número e complemento, como a Caixa escreve.                                                         |
| `bairro`        | texto | Normalizado para maiúsculas, sem espaços nas pontas.                                                            |
| `cidade`        | texto | Município.                                                                                                      |
| `estado`        | texto | Sigla da UF.                                                                                                    |
| `descricao`     | texto | Tipo do imóvel e áreas, em texto corrido.                                                                       |
| `preco`         | float | Preço de venda em reais.                                                                                        |
| `avaliacao`     | float | Valor de avaliação em reais.                                                                                    |
| `desconto`      | float | Percentual de desconto sobre a avaliação.                                                                       |
| `financiamento` | texto | `Sim` ou `Não`.                                                                                                 |
| `modalidade`    | texto | Venda Direta Online, Leilão SFI, entre outras.                                                                  |
| `foto`          | texto | Vazio. Coluna herdada de versões anteriores.                                                                    |
| `latitude`      | float | Nulo quando a [geocodificação](geocodificacao.md) não resolve.                                                  |
| `longitude`     | float | Idem.                                                                                                           |

`preco`, `avaliacao` e `desconto` chegam da Caixa em formato brasileiro
(`99.743,11`) e são convertidos para float no download.
