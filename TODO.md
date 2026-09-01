# TODO

## Alta prioridade

### 1. Contornar o anti-bot da Caixa

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

### 2. Melhorar a precisão da geocodificação

Hoje 42,8% dos imóveis casam ao nível de logradouro; o resto cai para bairro ou
município. Duas frentes, em ordem de custo:

1. Usar a tabela `municipio_logradouro_numero_localidade` (616 MB) para casar
   também o número, hoje descartado. Muitos registros da Caixa trazem `N. 00`,
   então o ganho precisa ser medido antes de pagar o download.
2. Padronizar o logradouro além das sete abreviações atuais. O `geocodebr` tem
   uma camada de padronização bem mais completa, em R, que pode ser portada.

## Média prioridade

### 3. Versionar os CSVs baixados

Hoje o download reescreve `data/` dentro do runner e o resultado se perde. Vale
decidir se os CSVs continuam versionados (com um commit automático a cada run)
ou se saem do repositório e viram apenas insumo efêmero do Parquet.
