---
type: Distribution
title: Publicação no Internet Archive
description: Onde o Parquet é publicado e como consultá-lo sem baixar o arquivo
identifier: imoveis-caixa-economica-federal
---

# Publicação no Internet Archive

Cada execução publica dois arquivos:

| Arquivo                    | O que é                                                                 |
| -------------------------- | ----------------------------------------------------------------------- |
| `imoveis_geocoded.parquet` | O [dataset](dataset-imoveis.md) consolidado e geocodificado.            |
| `imoveis_csv_bruto.zip`    | Os CSVs exatamente como a [Caixa](fonte-caixa.md) os serviu, um por UF. |

O bruto vai junto porque é a fonte primária e some assim que a Caixa atualiza a
lista: o Parquet é derivado e pode ser regerado, o CSV daquele dia não. É
também por isso que os CSVs **não são versionados no repositório** — o git
guardaria para sempre cada retrato, num histórico que ninguém consulta, e o
Archive já versiona, dedupe e serve.

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
