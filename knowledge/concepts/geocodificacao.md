---
type: Procedure
title: Geocodificação dos endereços
description: Resolve endereços em coordenadas via Nominatim, com cobertura parcial conhecida
provider: Nominatim (OpenStreetMap)
coverage: parcial
---

# Geocodificação dos endereços

As listas da Caixa não trazem coordenadas. O pipeline monta
`endereco, bairro, cidade, estado` e consulta o Nominatim, que limita a uma
requisição por segundo.

Esta é a etapa mais frágil do [pipeline](pipeline.md):

- é síncrona e roda dentro do job de publicação;
- o cache é um SQLite local, descartado a cada execução do CI, então cada run
  recomeça do zero;
- endereços que o Nominatim não resolve ficam com `latitude` e `longitude`
  nulos.

Quem consome o [dataset](dataset-imoveis.md) deve tratar coordenada nula como
o caso normal, não como erro. A variável `GEOCODER_KEY` não é uma chave de API:
é o User-Agent enviado ao Nominatim.
