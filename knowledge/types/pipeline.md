---
type: Spec
title: Spec — Pipeline
description: Transformação automatizada que lê uma fonte e produz um artefato
---

# Pipeline

Processo automatizado que consome um [Source](source.md) e produz um
[Dataset](dataset.md). Descreve etapas e ordem, não implementação.

| Campo         | Obrigatório | Significado                                       |
| ------------- | ----------- | ------------------------------------------------- |
| `title`       | sim         | Nome do pipeline.                                 |
| `description` | sim         | O que ele faz, em uma frase.                      |
| `entrypoint`  | sim         | Caminho, no repositório, do script que o executa. |
| `schedule`    | sim         | Quando roda. `manual` é uma resposta válida.      |

O corpo deve enumerar as etapas na ordem de execução e ligar cada uma ao
conceito que a detalha. Etapas que podem ser puladas por flag devem dizê-lo.
