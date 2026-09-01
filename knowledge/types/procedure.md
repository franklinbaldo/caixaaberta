---
type: Spec
title: Spec — Procedure
description: Etapa nomeada de um pipeline, destacada quando tem comportamento próprio
---

# Procedure

Etapa que ganha documento próprio por ter comportamento que o consumidor do
dado precisa conhecer — cobertura parcial, dependência externa, limite de
taxa. Uma etapa trivial não vira `Procedure`; fica como item no
[Pipeline](pipeline.md).

| Campo         | Obrigatório | Significado                                                                                   |
| ------------- | ----------- | --------------------------------------------------------------------------------------------- |
| `title`       | sim         | Nome da etapa.                                                                                |
| `description` | sim         | O que ela faz e qual é a ressalva.                                                            |
| `provider`    | não         | Serviço externo do qual a etapa depende.                                                      |
| `coverage`    | não         | `total` ou `parcial`. Obrigatório quando a etapa pode não produzir resultado para toda linha. |

Quando `coverage` é `parcial`, o corpo deve dizer explicitamente como o
resultado incompleto aparece no [Dataset](dataset.md) — tipicamente, valores
nulos que são o caso normal e não erro.
