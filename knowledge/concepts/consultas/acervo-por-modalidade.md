---
type: Consulta
title: Acervo por modalidade e estado
description: Como o acervo se distribui entre as formas de venda
engine: DuckDB
---

# Acervo por modalidade e estado

Primeira consulta a rodar sobre o dataset: sem ela, qualquer estatística de
valor mistura preço fixo com lance mínimo.

```sql
.read imoveis_caixa.sql

SELECT
  estado,
  modalidade,
  count(*) AS imoveis,
  round(median(preco), 2) AS preco_mediano,
  round(avg(desconto), 1) AS desconto_medio
FROM imoveis_caixa
GROUP BY estado, modalidade
ORDER BY estado, imoveis DESC;
```

`preco_mediano` só é comparável dentro de uma mesma modalidade. O
`desconto_medio` inclui os zeros de propósito — ver
[desconto zero](../armadilhas/desconto-zero-nao-e-ausencia-de-dado.md).
