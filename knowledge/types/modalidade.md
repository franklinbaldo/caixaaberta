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

O `title` é chave de junção com o dado: precisa bater literalmente com o valor
da coluna, acentos incluídos. O corpo deve dizer o que o comprador aceita ao
entrar nesta modalidade — não o passo a passo operacional, que é da Caixa e
muda sem aviso.
