---
type: Spec
title: Spec — Modalidade
description: Forma de venda sob a qual a Caixa oferta um imóvel
---

# Modalidade

Regime jurídico da oferta. É o campo que mais muda a leitura de um imóvel: dois
registros com o mesmo preço e a mesma avaliação significam coisas diferentes
conforme a modalidade.

| Campo         | Obrigatório | Significado                                                                    |
| ------------- | ----------- | ------------------------------------------------------------------------------ |
| `title`       | sim         | Nome da modalidade exatamente como aparece na coluna `modalidade`.             |
| `description` | sim         | O que caracteriza esta forma de venda.                                         |
| `valor_label` | sim         | O que a coluna `preco` significa nesta modalidade: preço fixo ou lance mínimo. |
| `competitiva` | sim         | `sim` quando o valor final pode subir por disputa entre proponentes.           |

As modalidades são atributo de cada linha do [Dataset](dataset.md) e aparecem
na coluna `modalidade` do [Schema](schema.md).

Os conceitos descrevem as modalidades **da fonte atual**. A Caixa muda essa
lista com o tempo: os CSVs de 2022 versionados em `data/` trazem `1º Leilão SFI`, `2º Leilão SFI` e `Venda Direta Especial`, que não aparecem mais em 2026.
Modalidade extinta não ganha conceito — o aviso do relatório existe justamente
para que a diferença apareça em vez de passar batida.

O `title` é chave de junção com o dado: precisa bater literalmente com o valor
da coluna, acentos incluídos. `KNOWN_MODALIDADES`, em `src/reporter.py`, repete
esses títulos para que o relatório aponte modalidade não documentada;
`scripts/check_bundle_contract.py` recusa a divergência entre os dois. O corpo deve dizer o que o comprador aceita ao
entrar nesta modalidade — não o passo a passo operacional, que é da Caixa e
muda sem aviso.
