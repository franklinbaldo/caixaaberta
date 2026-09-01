---
type: Spec
title: Spec — Distribution
description: Onde um dataset é publicado e como consumi-lo de lá
---

# Distribution

Canal por onde o [Dataset](dataset.md) chega a quem o consome. Um dataset pode
ter mais de uma distribuição.

| Campo         | Obrigatório | Significado                                                                                                                                                                                                               |
| ------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `title`       | sim         | Nome do canal.                                                                                                                                                                                                            |
| `description` | sim         | Onde publica e como consultar.                                                                                                                                                                                            |
| `identifier`  | sim         | Identificador do item no serviço de destino. É a fonte de verdade: `scripts/check_bundle_contract.py` compara este valor com os defaults de `run_pipeline.py` e `generate_ddl.py` e com o `imoveis_caixa.sql` versionado. |

O corpo deve trazer o caminho mais curto do leitor até uma consulta — de
preferência executável, sem baixar o artefato inteiro — e as credenciais
necessárias para publicar, se houver.
