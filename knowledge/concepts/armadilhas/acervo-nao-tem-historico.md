---
type: Armadilha
title: O dataset permite acompanhar a evolução de um imóvel no tempo
description: A Caixa publica só o estado atual; imóvel vendido desaparece da lista
severity: alta
---

# O dataset permite acompanhar a evolução de um imóvel no tempo

**A leitura ingênua.** Como o `link` identifica o imóvel, dá para comparar
publicações e ver preço caindo, imóvel encalhando, mercado se movendo.

**Por que falha.** A fonte é um retrato, não um histórico. Um imóvel some da
lista quando é vendido — e também quando é retirado, suspenso ou realocado
entre modalidades. Desaparecimento não é evidência de venda, e é assim que
uma série temporal ingênua vira uma taxa de venda inventada.

**O que fazer.** Usar as
[publicações no Archive](../publicacao-archive.md) como retratos datados e
tratar saída da lista como censura à direita, não como evento observado.
