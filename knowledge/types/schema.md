---
type: Spec
title: Spec — Schema
description: Colunas de um artefato publicado e o significado de cada uma
---

# Schema

Descrição coluna a coluna de um artefato concreto. Separado do
[Dataset](dataset.md) porque muda em ritmo próprio: o dataset é estável, as
colunas evoluem.

| Campo                  | Obrigatório | Significado                                                                                        |
| ---------------------- | ----------- | -------------------------------------------------------------------------------------------------- |
| `title`                | sim         | Nome do esquema.                                                                                   |
| `description`          | sim         | Que artefato ele descreve.                                                                         |
| `artifact`             | sim         | Caminho do arquivo que tem estas colunas.                                                          |
| `colunas_obrigatorias` | sim         | Colunas sem as quais o artefato não pode ser publicado. É a lista que o gate de publicação aplica. |

`colunas_obrigatorias` não é redundante com a tabela do corpo: a tabela
descreve tudo que o artefato tem, este campo declara o subconjunto cuja
ausência invalida a publicação. `scripts/check_bundle_contract.py` compara o
campo com `REQUIRED_PUBLICATION_COLUMNS` em `src/reporter.py`.

O corpo deve ser uma tabela com coluna, tipo e significado. Colunas herdadas,
sempre vazias ou mantidas por compatibilidade devem ser marcadas como tal em
vez de omitidas — quem lê o Parquet vai encontrá-las de qualquer jeito.
