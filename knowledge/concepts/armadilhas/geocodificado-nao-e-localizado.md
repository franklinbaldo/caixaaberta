---
type: Armadilha
title: Latitude preenchida significa que o imóvel foi localizado
description: Um quarto das coordenadas é o centro do município, não o endereço
severity: alta
evidencia: dos 12.115 imóveis de 01/09/2026, 3.177 (26,2%) receberam coordenada com precisao=municipio, e o desvio mediano do acervo é de 740 metros
---

# Latitude preenchida significa que o imóvel foi localizado

**A leitura ingênua.** 99,8% das linhas têm `latitude` e `longitude`; o dataset
está geocodificado; dá para plotar tudo num mapa.

**Por que falha.** A [geocodificação](../geocodificacao.md) é uma cascata, e
`latitude` preenchida só diz que *alguma* etapa casou. Na medição de
01/09/2026:

| `precisao`              | Imóveis | Fração |
| ----------------------- | ------- | ------ |
| `logradouro_localidade` | 2.257   | 18,6%  |
| `logradouro`            | 2.926   | 24,2%  |
| `localidade`            | 3.733   | 30,8%  |
| `municipio`             | 3.177   | 26,2%  |

Só **42,8%** chegam ao nível de rua. Mais de um quarto recebe o centro do
município — em municípios grandes, isso põe o imóvel a dezenas de quilômetros
do endereço real; o maior `desvio_metros` do acervo passa de 500 km. Um mapa
feito sem filtro mostra milhares de imóveis empilhados no centro geométrico das
cidades, e uma análise de distância a partir dele é ficção.

**O que fazer.** Filtrar por `precisao` antes de qualquer uso espacial. Para
mapa ou distância, exigir `precisao IN ('logradouro_localidade', 'logradouro')`.
Para agregação por município ou bairro, os níveis grosseiros servem — porque aí
a coordenada e a unidade de análise têm a mesma granularidade.
