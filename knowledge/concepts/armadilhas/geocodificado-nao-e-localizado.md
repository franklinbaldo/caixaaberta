---
type: Armadilha
title: Latitude preenchida significa que o imóvel foi localizado
description: Um quarto das coordenadas é o centro do município, não o endereço
severity: alta
evidencia: dos 25.687 imóveis de 01/09/2026, 2.524 (9,8%) receberam coordenada com precisao=municipio, cujo desvio médio no acervo publicado é de 18,7 km
---

# Latitude preenchida significa que o imóvel foi localizado

**A leitura ingênua.** 99,9% das linhas têm `latitude` e `longitude`; o dataset
está geocodificado; dá para plotar tudo num mapa.

**Por que falha.** A [geocodificação](../geocodificacao.md) é uma cascata, e
`latitude` preenchida só diz que *alguma* etapa casou. Na medição de
01/09/2026:

| `precisao`              | Imóveis | Fração |
| ----------------------- | ------- | ------ |
| `logradouro_localidade` | 10.909  | 42,5%  |
| `logradouro`            | 6.015   | 23,4%  |
| `localidade`            | 6.203   | 24,1%  |
| `municipio`             | 2.524   | 9,8%   |

Só **65,9%** chegam ao nível de rua. Um décimo recebe o centro do município —
em municípios grandes, isso põe o imóvel a dezenas de quilômetros do endereço
real. No dataset publicado, o `desvio_metros` médio é de 396 m nos dois níveis
de logradouro e de **18,7 km** em `municipio`. Um mapa
feito sem filtro mostra milhares de imóveis empilhados no centro geométrico das
cidades, e uma análise de distância a partir dele é ficção.

**O que fazer.** Filtrar por `precisao` antes de qualquer uso espacial. Para
mapa ou distância, exigir `precisao IN ('logradouro_localidade', 'logradouro')`.
Para agregação por município ou bairro, os níveis grosseiros servem — porque aí
a coordenada e a unidade de análise têm a mesma granularidade.
