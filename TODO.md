# TODO

## Alta prioridade

### 1. Tirar a geocodificação do caminho crítico

`fetch_data.py` geocodifica linha a linha, de forma síncrona, dentro do job de
publicação. São dezenas de milhares de endereços e o Nominatim limita a uma
requisição por segundo. O cache é um arquivo DuckDB local, descartado a cada
execução do CI, então todo run recomeça do zero. Por ser DuckDB, publicá-lo
como artefato é exportar uma tabela — não exige gravador próprio.

O que fazer:

1. Publicar o cache `endereco → latitude, longitude` como artefato próprio no
   Internet Archive e carregá-lo no início da execução.
2. Geocodificar apenas endereços novos, com um teto por execução.
3. Publicar o Parquet mesmo com geocodificação incompleta, registrando a
   cobertura no relatório.

### 2. Contornar o anti-bot da Caixa

A Caixa serve o CSV atrás do Radware Bot Manager, que devolve HTTP 200 com uma
página de bloqueio. Em medição de 01/09/2026, cerca de 6 em 8 requisições de um
IP de datacenter foram bloqueadas; User-Agent de navegador não ajudou. Com seis
tentativas e espera crescente, 1 de 3 estados passou.

Enquanto isso não for resolvido, o job de publicação roda com `--skip-fetch` e
segue republicando os CSVs versionados. O download é acionado à mão pelo input
`run_fetch` do workflow. Ligá-lo por padrão exige uma execução no Actions que
conclua as 27 UFs.

O que investigar, em ordem de custo:

1. Medir a taxa de sucesso a partir do runner do GitHub Actions, que tem IP e
   reputação diferentes deste ambiente.
2. Espaçar os estados ao longo de uma janela maior, em vez de 27 requisições em
   sequência.
3. Avaliar o endpoint `busca-imovel.asp` como alternativa ao arquivo estático.

## Média prioridade

### 3. Versionar os CSVs baixados

Hoje o download reescreve `data/` dentro do runner e o resultado se perde. Vale
decidir se os CSVs continuam versionados (com um commit automático a cada run)
ou se saem do repositório e viram apenas insumo efêmero do Parquet.
