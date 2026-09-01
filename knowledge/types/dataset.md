---
type: Spec
title: Spec — Dataset
description: Coleção de dados produzida por este projeto
---

# Dataset

O que o projeto produz e publica. Um `Dataset` é o conceito central: fontes
apontam para ele, esquemas o descrevem, distribuições o entregam.

| Campo         | Obrigatório | Significado                                                                   |
| ------------- | ----------- | ----------------------------------------------------------------------------- |
| `title`       | sim         | Nome do dataset.                                                              |
| `description` | sim         | O que uma linha representa, em uma frase.                                     |
| `format`      | sim         | Formato do artefato publicado.                                                |
| `grain`       | sim         | Granularidade: o que é uma linha e como a duplicidade é tratada.              |
| `license`     | sim         | Licença. Quando a fonte não declara uma, dizer isso em vez de omitir o campo. |

O corpo deve delimitar o que o dataset **não** é. A confusão mais cara sobre
dado público é de escopo, não de esquema.
