---
type: Pipeline
title: Pipeline de consolidação
description: Baixa as listas por estado, une com Ibis sobre DuckDB e grava um Parquet
entrypoint: src/run_pipeline.py
schedule: manual e a cada push em main
---

# Pipeline de consolidação

Quatro etapas, nesta ordem:

1. **Download** — `fetch_all_states` busca a lista de cada UF na
   [fonte da Caixa](fonte-caixa.md) e reescreve `data/imoveis_<UF>.csv`. É
   all-or-nothing: os arquivos só são gravados depois que todos os estados
   voltam com linhas. Cada estado é tentado até seis vezes, com espera crescente, por causa do
   anti-bot descrito na [fonte](fonte-caixa.md). Pode ser pulado com
   `--skip-fetch`.
2. **União** — `process_local_data` carrega os CSVs em DuckDB via Ibis, une as
   tabelas, normaliza `bairro`, descarta linhas sem `link` e deduplica.
3. **[Geocodificação](geocodificacao.md)** — preenche `latitude` e `longitude`
   para os endereços que não as têm.
4. **[Publicação](publicacao-archive.md)** — valida o Parquet e envia ao
   Internet Archive.

O resultado é [um único Parquet](dataset-imoveis.md), não uma tabela por
estado.
