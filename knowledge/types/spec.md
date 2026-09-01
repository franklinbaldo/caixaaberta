---
type: Spec
title: Spec — Spec
description: Documento que define um tipo de conceito deste bundle
---

# Spec

Cada `type` usado por um conceito deste bundle tem um documento aqui, e este
documento define o próprio tipo `Spec` — a definição fecha sobre si mesma, e é
por isso que o tipo aparece na sua própria lista.

| Campo         | Obrigatório | Significado                            |
| ------------- | ----------- | -------------------------------------- |
| `title`       | sim         | `Spec — <Tipo>`.                       |
| `description` | sim         | O que o tipo representa, em uma frase. |

O corpo deve trazer uma tabela dos campos de frontmatter do tipo, marcando
quais são obrigatórios, e uma regra editorial: o que um documento daquele tipo
precisa dizer e costuma esquecer.

Os tipos deste bundle: [Source](source.md), [Pipeline](pipeline.md),
[Procedure](procedure.md), [Dataset](dataset.md), [Schema](schema.md),
[Distribution](distribution.md), [Modalidade](modalidade.md),
[Armadilha](armadilha.md) e [Consulta](consulta.md).

O arquivo `<slug>.schema.sql` ao lado declara os campos observados como colunas
e alimenta a projeção relacional do bundle.

O CI valida esta correspondência com
`okf-parser check knowledge --require-spec 'types/{slug}.md' --normative-spec`:
um tipo novo sem spec quebra o build.
