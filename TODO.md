# TODO

## Alta prioridade

### 1. Ligar o download por padrão na publicação

O anti-bot foi contornado e as 27 UFs vêm em 86 segundos (ver
`knowledge/concepts/fonte-caixa.md`). Falta a prova a partir do runner do
GitHub Actions com o código novo: rodar o workflow com `run_fetch` e, se
concluir as 27, trocar o `--skip-fetch` do job de publicação pelo download.

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
