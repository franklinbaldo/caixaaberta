# Caixa Aberta

O Caixa Aberta observa diariamente os imóveis à venda da Caixa Econômica
Federal nas 27 UFs, consolida o retrato nacional em Parquet, geocodifica os
endereços e preserva cada dia no Internet Archive.

A Caixa publica o estado corrente, mas não oferece o próprio histórico. O valor
do projeto é repetir a observação de forma confiável: cada snapshot tem nome
datado e carrega `scrape_date` dentro do próprio dado.

## Consultar o Brasil inteiro

A superfície principal é DuckDB + Parquet remoto. Não é preciso baixar ou
manter banco local.

Para abrir o **último snapshot publicado**:

```sql
.read imoveis_caixa.sql

SELECT estado, count(*) AS imoveis
FROM imoveis_caixa
GROUP BY estado
ORDER BY imoveis DESC;
```

`imoveis_caixa.sql` resolve o último snapshot que realmente chegou ao Internet
Archive. Se a coleta de hoje falhou, a view continua apontando para o último dia
bem-sucedido.

Algumas consultas nacionais úteis:

```sql
-- uma cidade
SELECT *
FROM imoveis_caixa
WHERE estado = 'RO' AND upper(cidade) = 'PORTO VELHO';

-- venda direta com desconto alto
SELECT estado, cidade, endereco, preco, avaliacao, desconto, link
FROM imoveis_caixa
WHERE modalidade = 'Venda Direta Online' AND desconto >= 40
ORDER BY desconto DESC;

-- apenas coordenadas no nível de rua
SELECT *
FROM imoveis_caixa
WHERE precisao IN ('logradouro_localidade', 'logradouro');
```

### Consultar um dia histórico

Quem escolhe a data não precisa conhecer como os itens anuais estão organizados
no Archive. O gerador resolve a URL do snapshot:

```bash
uv run python src/generate_ddl.py --data 2026-09-02 --output-file /tmp/imoveis-2026-09-02.sql
duckdb
```

E no DuckDB:

```sql
.read /tmp/imoveis-2026-09-02.sql
SELECT scrape_date, count(*) FROM imoveis_caixa GROUP BY scrape_date;
```

Os retratos se acumulam em itens anuais no Internet Archive apenas como detalhe
de armazenamento. A superfície de consulta permanece a mesma.

## Como o histórico é produzido

O GitHub Actions executa o pipeline completo **uma vez por dia**, às 06:17 UTC,
e também permite execução manual por `workflow_dispatch`. Pull requests e
pushes em `main` validam código, mas não criam uma nova observação: o relógio,
não a atividade do Git, define a série histórica.

Cada execução bem-sucedida publica:

- `imoveis_geocoded_AAAA-MM-DD.parquet` — o retrato nacional processado;
- `imoveis_csv_bruto_AAAA-MM-DD.zip` — os 27 CSVs exatamente como a Caixa os
  serviu naquele scraping.

O Parquet carrega `scrape_date`, a data controlada pelo Caixa Aberta. A `Data de
geração` declarada pela Caixa continua preservada no ZIP bruto como
proveniência secundária.

## O download e o anti-bot da Caixa

A Caixa serve os CSVs atrás do Radware Bot Manager, que pode responder HTTP 200
com uma página de bloqueio no lugar do arquivo. O pipeline reconhece essa
resposta, usa um conjunto coerente de cabeçalhos de navegador, abre sessão nova
a cada requisição e percorre os estados em rodadas em vez de insistir no mesmo
estado.

A coleta é all-or-nothing: se uma UF não puder ser obtida, o pipeline falha em
vez de publicar um Brasil parcial com aparência de snapshot completo.

## Documentação do dataset

`knowledge/` é um bundle OKF com a fonte, o pipeline, o esquema do Parquet, a
publicação, modalidades de venda, armadilhas de interpretação e consultas
prontas. O CI verifica o bundle e o contrato entre documentação e código:

```bash
uvx --from okf-parser==0.45.2 okf-parser check knowledge \
  --require-spec 'types/{slug}.md' --normative-spec
uv run scripts/check_bundle_contract.py
```

Trabalho pendente vive em GitHub Issues. O repositório não mantém `TODO.md`
paralelo ao backlog.

## Pré-requisitos

- Python 3.10 ou superior
- `uv`
- DuckDB CLI para usar os exemplos com `.read`

## Instalação

```bash
uv venv .venv
uv pip install --python .venv/bin/python -e '.[dev]'
```

## Configuração

Copie `.env.sample` para `.env` e preencha o que for usar:

- `IA_ACCESS_KEY` e `IA_SECRET_KEY`: credenciais do Internet Archive para
  publicação real;
- `IA_COLLECTION`: coleção opcional do item, somente se a conta tiver
  privilégio de escrita nela;
- `URL_BASE`: molde da URL da lista por estado, com `{}` no lugar da UF.

## Rodar localmente

Pipeline completo em dry-run:

```bash
.venv/bin/python src/run_pipeline.py --upload-dry-run
```

Opções operacionais:

- `--skip-fetch`: usa CSVs já presentes em `data/`;
- `--skip-processing`: republica o Parquet existente;
- `--skip-upload`: coleta e processa sem publicar;
- `--data AAAA-MM-DD`: seleciona a data operacional de uma republicação;
- `--archive-item-identifier`, `--archive-item-title` e
  `--archive-item-description`: overrides avançados de publicação.

Antes de qualquer upload, o gate verifica arquivo legível, não vazio, schema
obrigatório, pelo menos um `link`, um único `scrape_date` e concordância entre
a data interna e o nome do snapshot.

## Relatório

```bash
.venv/bin/python src/reporter.py
```

O relatório usa o snapshot local datado mais recente e mostra volume, preços,
modalidades e precisão da geocodificação.

## Testes

```bash
.venv/bin/python -m pytest tests/
```

## Estrutura

| Caminho | Papel |
| --- | --- |
| `data/` | CSVs normalizados de entrada, um por estado |
| `src/fetch_data.py` | Coleta, união, limpeza e geração do snapshot Parquet |
| `src/geocode_cnefe.py` | Geocodificação pelo CNEFE em DuckDB |
| `src/reporter.py` | Gate de publicação e relatório |
| `src/run_pipeline.py` | Orquestra scraping, processamento e publicação |
| `src/upload_to_archive.py` | Publicação no Internet Archive |
| `src/generate_ddl.py` | Gera a view DuckDB atual ou histórica |
| `knowledge/` | Contrato e documentação do produto de dados |
| `.github/workflows/main.yml` | Validação e execução diária do pipeline |

## Licença

MIT.
