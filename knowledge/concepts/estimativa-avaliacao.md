---
type: Procedure
title: Estimativa independente de avaliação
description: Baseline reproduzível que aprende uma proxy de avaliação por localização e tipo sem usar o preço mínimo como feature
coverage: parcial
---

# Estimativa independente de avaliação

O primeiro modelo próprio do Caixa Aberta usa a série histórica dos snapshots
para estimar a **avaliação da Caixa como proxy**, não um preço de mercado
realizado. Essa distinção é parte do contrato: enquanto não houver target melhor,
a UI e os artefatos não podem chamar o resultado de “valor justo”, “valor de
mercado observado” nem “preço provável de venda”.

O baseline `caixa-avaliacao-proxy-hmedian-v0` é deliberadamente simples e
auditável. Ele aprende medianas e faixas empíricas em uma hierarquia de
localização + tipo de imóvel, recuando para grupos mais amplos até o fallback
global quando não há amostra local suficiente.

## Independência do lance mínimo

`preco`/lance mínimo **não entra nas features** do estimador. O preço só é
usado depois da inferência para calcular a distância entre a proxy estimada e o
que a Caixa está pedindo. Se o lance mínimo fosse usado para prever a própria
estimativa, o “sinal de oportunidade” seria parcialmente circular.

O derivado expõe:

- `estimativa_avaliacao`;
- `faixa_inferior` e `faixa_superior` empíricas do grupo usado;
- `escopo_estimativa` e `n_treino_escopo`;
- `model_id` e target explícito `avaliacao_caixa_proxy`;
- `gap_preco_abs` e `gap_preco_pct` contra o preço mínimo observado.

## Série histórica sem pseudorreplicação

Um mesmo imóvel pode permanecer em dezenas de snapshots. Para não fingir que
isso são dezenas de exemplos independentes, o dataset de treino usa uma única
observação por `link`: a primeira aparição conhecida.

O benchmark também é temporal. Imóveis cuja primeira aparição está no holdout
não podem aparecer no treino. Métricas globais e, quando há amostra suficiente,
por UF/cidade medem erro fora da amostra.

## Cobertura parcial

Cobertura parcial é esperada. Grupos locais pequenos recuam para níveis mais
amplos; versões futuras podem escolher recusar a estimativa quando o erro do
segmento for alto. Cobertura nacional não é requisito para promover um modelo.

## Evolução

Este baseline é o controle experimental para modelos posteriores: Kaggle,
CatBoost/XGBoost/LightGBM, comparáveis espaciais, modelos regionais ou ensembles.
Um modelo mais complexo só deve substituí-lo quando superar o baseline em
holdout temporal/geográfico sem apagar a proveniência do target.

Quando surgir fonte confiável de preço efetivo de venda/arrematação, ela deve
ser tratada como target separado. Desaparecer do snapshot nunca equivale a
venda observada.

## Reproduzir

```bash
uv run python scripts/valuation_baseline.py \
  output_data/imoveis_geocoded_*.parquet \
  --model-output output_data/modelo_avaliacao.parquet \
  --metrics-output output_data/benchmark_avaliacao.parquet \
  --score-snapshot output_data/imoveis_geocoded_2026-09-02.parquet \
  --estimates-output output_data/estimativas_2026-09-02.parquet
```
