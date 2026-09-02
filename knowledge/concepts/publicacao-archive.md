---
type: Distribution
title: Publicação no Internet Archive
description: Onde o Parquet é publicado e como consultá-lo sem baixar o arquivo
identifier_prefix: imoveis-caixa-economica-federal
manifesto: latest.json
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
o nome carrega a data: **nenhum arquivo de dado tem nome estável**. O endereço
do retrato mais recente vive num manifesto à parte, descrito adiante.

Os retratos se acumulam em **um item por ano** — `imoveis-caixa-economica-federal-2026`,
`-2027`, e assim por diante — para nenhum item crescer sem limite.

A data é propriedade da **execução**, lida uma vez no início do
[pipeline](pipeline.md) e passada adiante. Se cada etapa consultasse o relógio,
uma execução atravessando a meia-noite gravaria o zip num dia e o Parquet no
outro; na virada do ano, mandaria o retrato de 31/12 para o item de 2027. O
fuso é UTC, sempre: o produtor roda em runner UTC e o consumidor pode estar em
qualquer lugar. `--data` fixa a data, para republicação e reprodutibilidade.

Publica-se o par exato daquela data, nunca o que estiver no diretório: varrer
`output_data/` deixaria o Parquet de hoje passar pelo gate acompanhado do bruto
de ontem, e mandaria retratos velhos para o item novo na virada do ano.

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

## O ponteiro para o último retrato

"O mais recente" não é derivável do calendário. Duas razões: a publicação do
dia pode falhar, e aí "hoje" aponta para um arquivo que não existe; e o dia
corrente no DuckDB é o dia no **fuso da sessão de quem consulta**, então em
UTC-3 ou UTC+9 o consumidor calcularia outro dia ao redor da meia-noite — e
outro item inteiro na virada do ano.

Por isso existe um item sem ano, `imoveis-caixa-economica-federal`, que não
guarda dado: guarda `latest.json`, o único nome sobrescrito a cada publicação.

```json
{
  "data": "2026-09-02",
  "item": "imoveis-caixa-economica-federal-2026",
  "parquet_url": "https://archive.org/download/imoveis-caixa-economica-federal-2026/imoveis_geocoded_2026-09-02.parquet",
  "bruto_url": "https://archive.org/download/imoveis-caixa-economica-federal-2026/imoveis_csv_bruto_2026-09-02.zip"
}
```

Ele é publicado **depois** do upload dos dados, para o ponteiro só prometer
arquivo que existe, e é **monotônico**: republicar um retrato histórico com
`--data` não rebaixa o mais recente. Maior data publicada vence; empate
sobrescreve, porque republicar o mesmo dia é corrigir aquele dia. O ponteiro
também não segue `--archive-item-identifier`: publicar num item arbitrário é um
experimento, e um experimento não redireciona quem consulta o dataset.

Duas execuções simultâneas disputariam o ponteiro, e a que terminasse primeiro
perderia — por isso o workflow declara `concurrency` sobre a publicação.

O DDL em `imoveis_caixa.sql` cria uma view DuckDB que lê o manifesto e, dele, o
Parquet — direto do Archive, sem download do arquivo inteiro. `SET VARIABLE`
existe porque `read_parquet` não aceita subconsulta. Nada ali depende do
relógio nem do fuso, e nada precisa ser regerado, nem na virada do ano:

```sql
.read imoveis_caixa.sql
SELECT estado, count(*) FROM imoveis_caixa GROUP BY estado;
```

Para congelar um retrato específico da série:
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
