---
type: Consulta
title: Maiores descontos por estado, só em preço fixo
description: Onde estão as ofertas de preço fixo com maior abatimento sobre a avaliação
engine: DuckDB
---

# Maiores descontos por estado, só em preço fixo

Restringe à [Venda Direta Online](../modalidades/venda-direta-online.md), a
única modalidade em que `preco` e `desconto` descrevem uma venda de fato — as
demais anunciam lance mínimo.

```sql
.read imoveis_caixa.sql

SELECT estado, cidade, endereco, preco, avaliacao, desconto
FROM imoveis_caixa
WHERE modalidade = 'Venda Direta Online'
  AND desconto >= 30
ORDER BY desconto DESC
LIMIT 50;
```

Trocar a cláusula de modalidade por uma condição só de `desconto` cai direto em
[preço não é preço de venda](../armadilhas/preco-nao-e-preco-de-venda.md).
