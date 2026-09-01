---
type: Spec
title: Spec — Source
description: Origem externa de dados que este projeto consome sem controlar
---

# Source

Origem de dados fora do controle deste projeto. Um `Source` descreve de onde o
dado vem e sob que condições, não o que fazemos com ele.

| Campo         | Obrigatório | Significado                                                                                 |
| ------------- | ----------- | ------------------------------------------------------------------------------------------- |
| `title`       | sim         | Nome da fonte como um leitor a reconheceria.                                                |
| `description` | sim         | Uma frase sobre o que a fonte publica.                                                      |
| `publisher`   | sim         | Quem publica. Instituição, não sistema.                                                     |
| `url_pattern` | não         | Molde da URL, com o parâmetro variável entre chaves. Ausente quando o acesso não é por URL. |

Um `Source` deve registrar o que **não** é garantido: encoding, estabilidade de
esquema, cadência. É a informação que o consumidor precisa para decidir quanto
confiar no dado derivado.
