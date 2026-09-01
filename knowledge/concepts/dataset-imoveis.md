---
type: Dataset
title: Imóveis à venda da Caixa
description: Consolidação nacional das listas por estado, em Parquet, com coordenadas quando disponíveis
format: Parquet
grain: um imóvel por linha, deduplicado por conteúdo
license: dado público republicado; sem licença declarada pela fonte
---

# Imóveis à venda da Caixa

Uma linha por imóvel anunciado, para todas as 27 unidades federativas, com
[esquema estável](esquema-parquet.md) e coordenadas preenchidas quando a
[geocodificação](geocodificacao.md) resolve o endereço.

O arquivo é um retrato do dia em que o [pipeline](pipeline.md) rodou. A Caixa
não publica histórico: imóveis vendidos desaparecem da lista — e tratar isso
como série temporal é [uma armadilha](armadilhas/acervo-nao-tem-historico.md). Cada
[publicação no Archive](publicacao-archive.md) preserva um desses retratos.

Antes de qualquer estatística de valor, segmentar por modalidade: só a
[Venda Direta Online](modalidades/venda-direta-online.md) tem preço fixo. O
caminho mais curto é a consulta de
[acervo por modalidade](consultas/acervo-por-modalidade.md).

Não é um cadastro imobiliário. É o que a Caixa expõe sobre os imóveis que ela
própria está vendendo — retomados, em leilão ou em venda direta.
