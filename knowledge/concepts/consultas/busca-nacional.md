---
type: Consulta
title: Busca nacional de imóveis
description: Filtros diretos por UF, cidade, modalidade, desconto e precisão geográfica
engine: DuckDB
---

# Busca nacional de imóveis

A view `imoveis_caixa` representa um snapshot nacional inteiro. Depois de
`.read imoveis_caixa.sql`, os filtros não precisam conhecer item, arquivo ou
organização física do Internet Archive.

## Estado e cidade

```sql
SELECT scrape_date, link, endereco, bairro, cidade, estado, preco, modalidade
FROM imoveis_caixa
WHERE estado = 'RO'
  AND upper(cidade) = 'PORTO VELHO'
ORDER BY preco;
```

## Venda direta por desconto

```sql
SELECT scrape_date, estado, cidade, endereco, preco, avaliacao, desconto, link
FROM imoveis_caixa
WHERE modalidade = 'Venda Direta Online'
  AND desconto >= 40
ORDER BY desconto DESC, estado, cidade;
```

## Só coordenadas no nível de rua

```sql
SELECT scrape_date, estado, cidade, endereco, latitude, longitude, precisao, link
FROM imoveis_caixa
WHERE precisao IN ('logradouro_localidade', 'logradouro');
```

`preco` não tem a mesma semântica em todas as modalidades. Antes de comparar
valores entre grupos, ver [acervo por modalidade](acervo-por-modalidade.md) e a
armadilha [preço não é preço de venda](../armadilhas/preco-nao-e-preco-de-venda.md).
