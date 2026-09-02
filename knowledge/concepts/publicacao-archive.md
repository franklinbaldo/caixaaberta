---
type: Distribution
title: Publicação no Internet Archive
description: Onde o Parquet é publicado e como consultá-lo sem baixar o arquivo
identifier_prefix: imoveis-caixa-economica-federal
---

# Publicação no Internet Archive

Cada execução publica dois arquivos, ambos datados:

| Arquivo                               | O que é                                                                 |
| ------------------------------------- | ----------------------------------------------------------------------- |
| `imoveis_geocoded_AAAA-MM-DD.parquet` | O [dataset](dataset-imoveis.md) consolidado daquele dia.                |
| `imoveis_csv_bruto_AAAA-MM-DD.zip`    | Os CSVs como a [Caixa](fonte-caixa.md) os serviu naquele dia, um por UF.|

O acervo da Caixa é um retrato do dia: imóvel vendido some da lista e a fonte
não guarda histórico. Se toda publicação sobrescrevesse um nome fixo, a série
temporal que este projeto existe para preservar seria destruída a cada
execução — o Archive não versiona arquivo homônimo dentro de um item. Por isso
o nome carrega a data, e **não existe apelido para o mais recente**: quem
consulta calcula a data.

Os retratos se acumulam em **um item por ano** — `imoveis-caixa-economica-federal-2026`,
`-2027`, e assim por diante — para nenhum item crescer sem limite.

O bruto vai junto porque é a fonte primária e some assim que a Caixa atualiza a
lista: o Parquet é derivado e pode ser regerado, o CSV daquele dia não. É
também por isso que os CSVs **não são versionados no repositório** — o git
guardaria para sempre cada retrato, num histórico que ninguém consulta, e o
Archive já versiona, dedupe e serve.

Isso é um gate, não uma recomendação: `upload_files_to_archive` recusa publicar
sem o zip. A única exceção é `--skip-processing`, que republica um Parquet já
existente e portanto não tem bruto novo a preservar — e a declara passando
`exigir_bruto=False`, em vez de deixar a ausência passar em silêncio.

O [Parquet](dataset-imoveis.md) é enviado ao item do ano corrente. Antes do envio,
`validate_publication_parquet` recusa arquivo ausente, arquivo vazio, esquema
incompleto ou um conjunto em que nenhuma linha tem `link` publicável — o
[pipeline](pipeline.md) falha em vez de publicar dado inútil.

O DDL em `imoveis_caixa.sql` cria uma view DuckDB que lê o Parquet direto do
Archive, sem download do arquivo inteiro. Ele não guarda data nem ano: monta a
URL em SQL a partir de `current_date`, e o DuckDB dobra a expressão no bind.
Nem a virada do dia nem a do ano exigem regerar o arquivo.

```sql
.read imoveis_caixa.sql
SELECT estado, count(*) FROM imoveis_caixa GROUP BY estado;
```

A contrapartida é que a view aponta para o retrato de hoje, que só existe
depois da publicação do dia. Para congelar um retrato específico:
`python src/generate_ddl.py --data 2026-09-02`.

Publicar exige `IA_ACCESS_KEY` e `IA_SECRET_KEY`; `--upload-dry-run` dispensa
as duas.

O Archive raciona uploads quando **a fila global dele** se aproxima do teto:

```
Please reduce your request rate. - total_tasks_queued exceeds global_limit
```

O limite não é do item nem da conta. Em 02/09/2026 a recusa veio com
`bucket_tasks_queued` e `accesskey_tasks_queued` zerados e
`total_tasks_queued` em 11.639 de 11.999, com `rationing_engaged: 1` — ou
seja, o Archive inteiro estava congestionado. Publicar menos não evita isso;
só esperar. O upload espera e tenta de novo, e nada é publicado pela metade
porque o envio dos dois arquivos é uma operação só.

O estado do racionamento é público em
`https://s3.us.archive.org/?check_limit=1`.

O item sobe **sem coleção declarada**. O Archive recusa o upload inteiro quando
o metadata nomeia uma coleção em que a conta não pode escrever — foi o que
aconteceu com `opensource_data`, em 01/09/2026, com credencial válida:

```
Access Denied - You lack sufficient privileges to write to those collections
```

Sem coleção, o item fica na área geral da conta e pode ser movido depois por
quem tenha o privilégio. `IA_COLLECTION` declara uma coleção quando a conta
tiver acesso a ela.
