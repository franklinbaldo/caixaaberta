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

A Caixa serve o CSV atrás do anti-bot Radware, que responde **HTTP 200 com uma
página HTML de bloqueio** no lugar do arquivo. Três coisas decidem se o pedido
passa, todas medidas em 01/09/2026:

| Escolha                                                | Passou   |
| ------------------------------------------------------ | -------- |
| Sem cabeçalho algum                                    | 3 de 10  |
| Só `User-Agent` de navegador, sem os demais cabeçalhos | 0 de 6   |
| Conjunto coerente de cabeçalhos de navegador           | 10 de 10 |
| Sessão HTTP reusada entre estados                      | 1 de 8   |
| Sessão nova a cada requisição                          | 6 de 8   |

O anti-bot avalia **coerência**, não a presença de um User-Agent: um UA de
navegador desacompanhado dos cabeçalhos que o navegador manda junto
(`Sec-Fetch-*`, `Accept-Language`, `Upgrade-Insecure-Requests`) é pior que
não fingir nada. E ele marca o cliente pelos cookies `__uzm*` que injeta na
primeira resposta — reenviá-los identifica quem já foi avaliado.

O [pipeline](pipeline.md) manda o conjunto coerente, abre sessão nova a cada
requisição e nunca insiste no mesmo estado em sequência. Com isso, as 27 UFs
vêm em 86 segundos, em uma única passagem.

Não há contrato de estabilidade: a Caixa pode mudar colunas, encoding ou
endereço sem aviso. O [pipeline](pipeline.md) trata qualquer estado que não
retorne linhas como falha, em vez de publicar um recorte parcial.
