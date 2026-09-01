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
   voltam com linhas. Os estados são percorridos em rodadas: um bloqueio devolve o estado à fila em
   vez de insistir nele, que é o que o anti-bot da [fonte](fonte-caixa.md)
   pune. Pode ser pulado com
   `--skip-fetch`, e é pulado de todo modo sob `--skip-processing`, que não
   teria o que fazer com dado novo. A publicação automática roda com o
   download desligado — ver [fonte](fonte-caixa.md).
2. **União** — `process_local_data` carrega os CSVs em DuckDB via Ibis, une as
   tabelas, normaliza `bairro`, descarta linhas sem `link` e deduplica.
3. **[Geocodificação](geocodificacao.md)** — casa os endereços com o CNEFE em
   DuckDB e preenche `latitude`, `longitude`, `precisao` e `desvio_metros`.
4. **[Publicação](publicacao-archive.md)** — valida o Parquet e envia ao
   Internet Archive.

O resultado é [um único Parquet](dataset-imoveis.md), não uma tabela por
estado.
