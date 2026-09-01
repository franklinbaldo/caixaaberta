---
type: Procedure
title: Geocodificação dos endereços
description: Casa os endereços com o CNEFE em DuckDB, em cascata do logradouro ao município
provider: CNEFE/IBGE, na padronização do IPEA
coverage: parcial
---

# Geocodificação dos endereços

As listas da Caixa não trazem coordenadas. Elas vêm do Cadastro Nacional de
Endereços para Fins Estatísticos (CNEFE/IBGE), na versão padronizada que o IPEA
publica em Parquet para o pacote R
[geocodebr](https://github.com/ipeaGIT/geocodebr). O pacote é R, mas o
casamento de endereços dele é SQL sobre esses arquivos — aqui o dado é
consumido direto pelo DuckDB que o [pipeline](pipeline.md) já carrega.

O casamento é uma cascata: cada endereço tenta a chave mais específica e, se
falhar, cai para a seguinte. A coluna `precisao` registra onde ele parou.

| `precisao`              | Chave                                 | O que a coordenada é                     |
| ----------------------- | ------------------------------------- | ---------------------------------------- |
| `logradouro_localidade` | estado, município, logradouro, bairro | Um ponto na rua, no bairro certo.        |
| `logradouro`            | estado, município, logradouro         | Um ponto na rua, sem confirmar o bairro. |
| `localidade`            | estado, município, bairro             | Centro do bairro.                        |
| `municipio`             | estado, município                     | Centro do município.                     |

`desvio_metros` acompanha cada coordenada e vem do próprio CNEFE.

Não há chamada de rede no caminho crítico e não há cache a manter: os Parquets
do CNEFE são baixados uma vez por release e o casamento é um join. A versão
anterior consultava o Nominatim linha a linha, a uma requisição por segundo, e
resolvia 11,4% dos endereços em cinco horas e vinte minutos por execução. A
mesma base, pelo CNEFE, resolve 99,8% em menos de seis segundos — mas a leitura
desse número tem [armadilha própria](armadilhas/geocodificado-nao-e-localizado.md).
