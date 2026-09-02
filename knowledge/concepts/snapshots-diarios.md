---
type: Procedure
title: Snapshot diário
 description: Regra operacional que separa validação de código da observação diária da Caixa
---

# Snapshot diário

O relógio do Caixa Aberta é o agendamento diário, não a atividade do
repositório. Um `push` pode mudar o software sem dizer nada sobre o estado dos
imóveis; da mesma forma, um dia sem commits continua sendo um dia que precisa
ser observado.

A execução agendada percorre as 27 UFs, processa o retrato nacional, valida o
artefato e só então o publica. `workflow_dispatch` usa o mesmo caminho para
recuperação ou operação manual.

PRs e pushes em `main` executam validação, mas não publicam uma observação nova.
Se scraping ou publicação falham, o workflow falha e o último snapshot válido
permanece servido; não há fallback silencioso para um dado antigo com data
nova.
