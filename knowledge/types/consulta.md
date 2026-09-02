---
type: Spec
title: Spec — Consulta
description: Consulta SQL pronta contra o dataset publicado
---

# Consulta

Receita executável contra o Parquet no Internet Archive, sem baixar o arquivo.
Roda contra a [Distribution](distribution.md) do [Dataset](dataset.md).
Serve de porta de entrada e de teste vivo: uma consulta que para de funcionar
sinaliza mudança de esquema.

| Campo         | Obrigatório | Significado                             |
| ------------- | ----------- | --------------------------------------- |
| `title`       | sim         | O que a consulta responde.              |
| `description` | sim         | A pergunta em uma frase.                |
| `engine`      | sim         | Motor em que a consulta foi verificada. |

O corpo deve conter um único bloco SQL executável, precedido do DDL necessário,
e apontar as [armadilhas](armadilha.md) que a consulta evita ou nas quais o
leitor cairia se a alterasse sem cuidado.
