# Caixa Aberta

Consolida os dados de imóveis à venda da Caixa Econômica Federal em um único
arquivo Parquet e publica esse arquivo no Internet Archive.

O pipeline baixa a lista de imóveis de cada estado no site da Caixa, grava os
CSVs por estado em `data/`, une tudo com Ibis sobre DuckDB, geocodifica os
endereços sem coordenadas e grava `output_data/imoveis_geocoded.parquet`.

## Consumir os dados

Para consultar o dataset publicado sem baixar o arquivo inteiro, abra o DuckDB
e execute o DDL de `imoveis_caixa.sql`, que cria uma view lendo diretamente o
Parquet publicado no Internet Archive:

```sql
.read imoveis_caixa.sql
SELECT estado, count(*) FROM imoveis_caixa GROUP BY estado;
```

Os retratos diários se acumulam em um item por ano
(`imoveis-caixa-economica-federal-2026`), cada publicação sob um nome datado
— `imoveis_geocoded_2026-09-02.parquet` — porque a lista da Caixa é um retrato
do dia e o imóvel vendido some dela. Ao lado dos datados, `imoveis_geocoded.parquet`
e `imoveis_csv_bruto.zip` apontam sempre para a publicação mais recente; é
deles que o DDL lê.

Quando o ano vira, o DDL precisa ser regerado:

```bash
python src/generate_ddl.py            # item do ano corrente
python src/generate_ddl.py --identifier <ID_DO_ITEM>
```

### O download e o anti-bot da Caixa

A Caixa serve os CSVs atrás do Radware Bot Manager, que responde HTTP 200 com
uma página de bloqueio no lugar do arquivo. O pipeline contorna isso mandando
um conjunto coerente de cabeçalhos de navegador, abrindo sessão HTTP nova a
cada requisição e percorrendo os estados em rodadas em vez de insistir num
deles. As 27 UFs vêm em 86 segundos.

O download roda por padrão na publicação — confirmado no runner do GitHub em
01/09/2026, com as 27 UFs. Os CSVs **não são versionados**: `data/` está no
`.gitignore`, e cada publicação leva ao Archive tanto o Parquet quanto
o zip datado com os CSVs como a Caixa os serviu. O dado vive no
Archive; o repositório guarda código.

## Documentação do dataset

`knowledge/` é um bundle [OKF v0.2](https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md)
com 26 conceitos: a fonte, o pipeline, o esquema do Parquet, a publicação, as
quatro modalidades de venda, as armadilhas conhecidas do dado e consultas
prontas. É a documentação para quem consome o dataset sem ler o código.

O CI valida o bundle a cada push, exigindo definição de tipo para todo `type`
usado:

```bash
uvx --from okf-parser okf-parser check knowledge \
  --require-spec 'types/{slug}.md' --normative-spec
uvx --from okf-parser okf-parser graph knowledge
```

Três valores do bundle estão repetidos no código — o identificador do item no
Archive, as colunas obrigatórias para publicar e as modalidades de venda. A
repetição existe porque o `okf-parser` exige Python 3.12 e o pipeline suporta
3.10; importá-lo em produção custaria esse suporte. A divergência entre as
cópias é recusada no CI por um script isolado, com dependências declaradas em
PEP 723:

```bash
uv run scripts/check_bundle_contract.py
```

## Pré-requisitos

- Python 3.10 ou superior
- [uv](https://github.com/astral-sh/uv)

## Instalação

```bash
uv venv .venv
uv pip install --python .venv/bin/python -e '.[dev]'
```

## Configuração

Copie `.env.sample` para `.env` e preencha o que for usar:

- `IA_ACCESS_KEY` e `IA_SECRET_KEY`: credenciais do Internet Archive. Exigidas
  para publicar; dispensáveis com `--upload-dry-run`. Para conferir um par sem
  gastar uma execução, use `archive.org/services/user.php?op=whoami` — os
  endpoints do S3 do Archive respondem 200 mesmo sem credencial nenhuma e não
  servem para validar.
- `IA_COLLECTION`: coleção do item, opcional. Só declare uma em que a conta
  tenha privilégio de escrita: o Archive recusa o upload inteiro caso
  contrário.
- `URL_BASE`: molde da URL da lista por estado, com `{}` no lugar da UF. O
  padrão é `https://venda-imoveis.caixa.gov.br/listaweb/Lista_imoveis_{}.csv`.

## Rodar o pipeline

```bash
.venv/bin/python src/run_pipeline.py --upload-dry-run
```

Sem `--upload-dry-run`, o script publica no Internet Archive. Outras opções:

- `--skip-fetch`: pula o download e usa os CSVs já presentes em `data/`. Como
  `data/` não é versionado, só serve depois de um download anterior na mesma
  máquina. Implícito em `--skip-processing`.
- `--skip-processing`: pula o processamento e publica o Parquet já existente.
- `--skip-upload`: só processa, não publica.
- `--archive-item-identifier`, `--archive-item-title`,
  `--archive-item-description`: metadados do item no Archive.

Antes de publicar, `run_pipeline.py` valida o Parquet: o arquivo precisa
existir, ser legível, ter linhas, conter as colunas obrigatórias e ter pelo
menos um `link` preenchido. Se a validação falha, a publicação não acontece.

## Relatório

```bash
.venv/bin/python src/reporter.py
```

Imprime total de imóveis, contagem e preço médio por estado, e taxa de
geocodificação.

## Testes

```bash
.venv/bin/python -m pytest tests/
```

## Estrutura

| Caminho | Papel |
| --- | --- |
| `data/` | CSVs de entrada, um por estado |
| `src/fetch_data.py` | União, limpeza e geocodificação; gera o Parquet |
| `src/geocoding_utils.py` | Geocodificação com cache em SQLite |
| `src/reporter.py` | Validação de publicação e relatório |
| `src/run_pipeline.py` | Orquestra processamento, validação e publicação |
| `src/upload_to_archive.py` | Envio ao Internet Archive |
| `src/generate_ddl.py` | Gera `imoveis_caixa.sql` |
| `.github/workflows/main.yml` | Testes em PR; processa e publica em `main` |

## Licença

MIT.
