# TODO

## Alta prioridade

### 1. Melhorar a precisão da geocodificação

Hoje 65,9% dos imóveis casam ao nível de logradouro; o resto cai para bairro ou
município. Duas frentes, em ordem de custo:

1. Usar a tabela `municipio_logradouro_numero_localidade` (616 MB) para casar
   também o número, hoje descartado. Muitos registros da Caixa trazem `N. 00`,
   então o ganho precisa ser medido antes de pagar o download.
2. Padronizar o logradouro além das sete abreviações atuais. O `geocodebr` tem
   uma camada de padronização bem mais completa, em R, que pode ser portada.

## Média prioridade

