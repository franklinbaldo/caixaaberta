---
type: Armadilha
title: desconto igual a zero significa desconto não informado
description: Zero é o valor correto para o imóvel ofertado pela avaliação cheia
severity: media
evidencia: 40,1% dos 4.120 imóveis medidos em RO, SP e BA têm desconto exatamente 0, e nenhum tem desconto nulo
---

# `desconto` igual a zero significa desconto não informado

**A leitura ingênua.** Zero é sentinela de ausência; filtrar `desconto > 0`
limpa o dataset.

**Por que falha.** Zero é um valor legítimo e frequente: o imóvel está ofertado
pelo valor de avaliação. Na amostra medida, nenhum registro tem `desconto`
nulo — a ausência simplesmente não ocorre nesta coluna. Filtrar `> 0` descarta
40% do acervo e enviesa qualquer média de desconto para cima.

**O que fazer.** Tratar zero como zero. Se a intenção é achar oportunidade,
dizer isso no filtro (`desconto >= 20`), não escondê-la atrás de uma limpeza.
