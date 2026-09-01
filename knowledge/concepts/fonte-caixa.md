---
type: Source
title: Listas de imóveis da Caixa
description: Origem primária dos dados, uma lista CSV por unidade federativa
publisher: Caixa Econômica Federal
url_pattern: https://venda-imoveis.caixa.gov.br/listaweb/Lista_imoveis_{UF}.csv
---

# Listas de imóveis da Caixa

A Caixa publica, por unidade federativa, a lista dos imóveis que coloca à
venda. Cada lista é um CSV em `latin-1`, separado por `;`, com duas linhas de
preâmbulo antes do cabeçalho — a primeira delas carrega a data de geração.

O endpoint `.htm` do mesmo diretório, usado por versões anteriores deste
projeto, responde 404. Só o `.csv` está no ar.

Não há contrato de estabilidade: a Caixa pode mudar colunas, encoding ou
endereço sem aviso. O [pipeline](pipeline.md) trata qualquer estado que não
retorne linhas como falha, em vez de publicar um recorte parcial.
