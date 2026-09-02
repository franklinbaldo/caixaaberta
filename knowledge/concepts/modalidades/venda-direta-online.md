---
type: Modalidade
title: Venda Direta Online
description: Preço fixo, primeiro proponente leva, sem disputa
valor_label: preço de venda
competitiva: não
---

# Venda Direta Online

Oferta a preço fixo. Não há lance nem sessão pública: o primeiro proponente
que aceita as condições e habilita a documentação compra pelo valor anunciado.

Nesta modalidade `preco` (ver [esquema](../esquema-parquet.md)) é o preço final e `desconto` é o abatimento efetivo
sobre a avaliação — a única em que a leitura ingênua das colunas de valor está
correta. Comparações de "quanto está barato" só são seguras dentro dela.
