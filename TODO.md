# TODO

## Alta prioridade

### 1. Baixar os dados da Caixa

`src/fetch_data.py` não baixa nada: lê apenas os CSVs versionados em `data/`,
cujo último commit é de agosto de 2022. Cada execução do workflow republica os
mesmos dados de 2022 no Internet Archive.

O que fazer:

1. Implementar o download a partir de `URL_BASE`, uma requisição por estado.
2. Parsear o HTML da Caixa e gravar os CSVs em `data/`, sobrescrevendo.
3. Chamar o download no início de `run_pipeline.py`, atrás de uma flag
   `--skip-fetch` para permitir rodar offline.
4. Falhar a execução se algum estado não retornar linhas, em vez de publicar um
   Parquet parcial.

As dependências `requests`, `lxml`, `beautifulsoup4` e `html5lib` já estão
declaradas para isso e hoje não são usadas.

### 2. Alinhar o DDL ao Parquet produzido

`generate_ddl.py` gera um `read_parquet` de 27 arquivos por estado
(`imoveis_AC.parquet` e seguintes). O pipeline grava um arquivo único,
`imoveis_geocoded.parquet`. A view distribuída aponta para arquivos que não
existem no item.

Escolher um dos dois formatos e ajustar o outro lado.

### 3. Tirar a geocodificação do caminho crítico

`fetch_data.py` geocodifica linha a linha, de forma síncrona, dentro do job de
publicação. São dezenas de milhares de endereços e o Nominatim limita a uma
requisição por segundo. O cache é um SQLite local, descartado a cada execução
do CI, então todo run recomeça do zero.

O que fazer:

1. Publicar o cache `endereco → latitude, longitude` como artefato próprio no
   Internet Archive e carregá-lo no início da execução.
2. Geocodificar apenas endereços novos, com um teto por execução.
3. Publicar o Parquet mesmo com geocodificação incompleta, registrando a
   cobertura no relatório.

## Média prioridade

### 4. Remover arquivos mortos

`hello.py`, `src/test.py`, `exemplo_imoveis.csv`, `data/__init__.py` e o binário
versionado `caixa_imoveis_pipeline.duckdb`.

### 5. Cobrir o fetch com teste

Depois da tarefa 1, testar o parsing do HTML com `requests-mock`, sem rede.
