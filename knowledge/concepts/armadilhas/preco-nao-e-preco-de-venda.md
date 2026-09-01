---
type: Armadilha
title: A coluna preco é o preço pelo qual o imóvel será vendido
description: Em três das quatro modalidades preco é lance mínimo, não preço final
severity: alta
evidencia: 3 de 4 modalidades são competitivas; elas somam 2.884 dos 4.120 imóveis medidos em RO, SP e BA em 01/09/2026
---

# A coluna `preco` é o preço pelo qual o imóvel será vendido

**A leitura ingênua.** `preco` é quanto custa o imóvel; média de `preco` é o
preço médio do acervo.

**Por que falha.** Só a
[Venda Direta Online](../modalidades/venda-direta-online.md) tem preço fixo.
Nas outras três modalidades o campo é lance mínimo e o valor de fechamento não
existe no dataset — a Caixa não publica resultado de leilão. Qualquer média
sobre o acervo inteiro mistura preço final com piso de disputa e subestima o
custo real.

**O que fazer.** Segmentar por `modalidade` antes de qualquer estatística de
valor, e dizer no resultado que os valores das modalidades competitivas são
pisos.
