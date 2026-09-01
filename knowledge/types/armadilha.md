---
type: Spec
title: Spec — Armadilha
description: Leitura intuitiva do dado que produz conclusão errada
---

# Armadilha

Cada `Armadilha` documenta uma inferência que parece óbvia sobre o
[dataset](../concepts/dataset-imoveis.md) e está errada. São o conteúdo mais
valioso deste bundle: o esquema qualquer um deduz abrindo o Parquet; isto, não.

| Campo         | Obrigatório | Significado                                                                                       |
| ------------- | ----------- | ------------------------------------------------------------------------------------------------- |
| `title`       | sim         | A conclusão errada, enunciada afirmativamente.                                                    |
| `description` | sim         | Por que ela falha, em uma frase.                                                                  |
| `severity`    | sim         | `alta` quando a leitura errada inverte o sinal de uma análise; `media` quando distorce magnitude. |
| `evidencia`   | não         | Medição que sustenta a armadilha, com amostra e data.                                             |

Uma [Consulta](consulta.md) que evita uma armadilha deve dizer qual.

O corpo deve trazer a leitura ingênua, a razão de ela falhar e o que fazer no
lugar. Uma armadilha sem contramedida é uma reclamação, não documentação.
