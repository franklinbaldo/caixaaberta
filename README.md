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

Para regerar esse DDL apontando para outro item do Archive:

```bash
python src/generate_ddl.py --identifier <ID_DO_ITEM>
```

## Documentação do dataset

`knowledge/` é um bundle [OKF v0.2](https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md)
que descreve a fonte, o pipeline, o esquema do Parquet e a publicação — para
quem consome o dado sem ler o código. O CI valida o bundle a cada push:

```bash
uvx --from okf-parser okf-parser check knowledge
uvx --from okf-parser okf-parser graph knowledge
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
  para publicar; dispensáveis com `--upload-dry-run`.
- `GEOCODER_KEY`: nome legado da variável usada como User-Agent do Nominatim
  por `src/geocoding_utils.py`; o valor deve identificar o cliente, de
  preferência com uma forma de contato. Não é uma API key do Nominatim.
- `URL_BASE`: molde da URL da lista por estado, com `{}` no lugar da UF. O
  padrão é `https://venda-imoveis.caixa.gov.br/listaweb/Lista_imoveis_{}.csv`.

## Rodar o pipeline

```bash
.venv/bin/python src/run_pipeline.py --upload-dry-run
```

Sem `--upload-dry-run`, o script publica no Internet Archive. Outras opções:

- `--skip-fetch`: pula o download e usa os CSVs já presentes em `data/`.
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
