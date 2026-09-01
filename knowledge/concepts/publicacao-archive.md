---
type: Distribution
title: Publicação no Internet Archive
description: Onde o Parquet é publicado e como consultá-lo sem baixar o arquivo
identifier: imoveis-caixa-economica-federal
---

# Publicação no Internet Archive

O [Parquet](dataset-imoveis.md) é enviado ao item
`imoveis-caixa-economica-federal` do Internet Archive. Antes do envio,
`validate_publication_parquet` recusa arquivo ausente, arquivo vazio, esquema
incompleto ou um conjunto em que nenhuma linha tem `link` publicável — o
[pipeline](pipeline.md) falha em vez de publicar dado inútil.

O DDL em `imoveis_caixa.sql` cria uma view DuckDB que lê o Parquet direto do
Archive, sem download do arquivo inteiro:

```sql
.read imoveis_caixa.sql
SELECT estado, count(*) FROM imoveis_caixa GROUP BY estado;
```

Publicar exige `IA_ACCESS_KEY` e `IA_SECRET_KEY`; `--upload-dry-run` dispensa
as duas.

O item sobe **sem coleção declarada**. O Archive recusa o upload inteiro quando
o metadata nomeia uma coleção em que a conta não pode escrever — foi o que
aconteceu com `opensource_data`, em 01/09/2026, com credencial válida:

```
Access Denied - You lack sufficient privileges to write to those collections
```

Sem coleção, o item fica na área geral da conta e pode ser movido depois por
quem tenha o privilégio. `IA_COLLECTION` declara uma coleção quando a conta
tiver acesso a ela.
