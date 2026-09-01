# TODO

## Alta prioridade

### 1. Tirar a geocodificação do caminho crítico

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

### 2. Versionar os CSVs baixados

Hoje o download reescreve `data/` dentro do runner e o resultado se perde. Vale
decidir se os CSVs continuam versionados (com um commit automático a cada run)
ou se saem do repositório e viram apenas insumo efêmero do Parquet.
