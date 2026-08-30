# Caixa Aberta

Consolida os dados de imóveis à venda da Caixa Econômica Federal em um único
arquivo Parquet e publica esse arquivo no Internet Archive.

O pipeline lê os CSVs por estado em `data/`, une tudo com Ibis sobre DuckDB,
geocodifica os endereços sem coordenadas e grava
`output_data/imoveis_geocoded.parquet`.

## Consumir os dados

Para consultar o dataset publicado sem baixar nada, abra o DuckDB e execute o
DDL de `imoveis_caixa.sql`, que cria uma view lendo os Parquet direto do
Internet Archive:

```sql
.read imoveis_caixa.sql
SELECT estado, count(*) FROM imoveis_caixa GROUP BY estado;
```

Para regerar esse DDL apontando para outro item do Archive:

```bash
python src/generate_ddl.py --identifier <ID_DO_ITEM>
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
- `GEOCODER_KEY`: chave do serviço de geocodificação usado por
  `src/geocoding_utils.py`.
- `URL_BASE`: origem dos dados da Caixa. Ainda não consumida pelo código —
  veja `TODO.md`.

## Rodar o pipeline

```bash
.venv/bin/python src/run_pipeline.py --upload-dry-run
```

Sem `--upload-dry-run`, o script publica no Internet Archive. Outras opções:

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
