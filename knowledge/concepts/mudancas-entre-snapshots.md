---
type: Dataset
title: Mudanças entre snapshots diários
description: Diferenças reproduzíveis entre duas observações consecutivas do acervo da Caixa
format: Parquet
grain: um imóvel com mudança observada entre dois snapshots
license: derivado dos snapshots publicados pelo Caixa Aberta
---

# Mudanças entre snapshots diários

Os snapshots são a verdade imutável. Esta camada é apenas uma derivação entre
D-1 e D e pode ser refeita a qualquer momento com `src/compare_snapshots.py`.

A reconciliação usa `link` como identificador prático do imóvel dentro de cada
snapshot. Se um mesmo `link` aparecer duas vezes no mesmo arquivo, a comparação
falha em vez de escolher uma linha arbitrariamente.

## Tipos de mudança

| `mudanca` | Significado |
| --- | --- |
| `entrou_no_estoque` | O `link` não estava no snapshot anterior e aparece no atual. |
| `saiu_do_estoque` | O `link` estava no anterior e não aparece no atual. |
| `alterou` | O imóvel aparece nos dois dias e mudou `preco`, `avaliacao`, `desconto` ou `modalidade`. |

`campos_alterados` lista quais desses campos mudaram. O Parquet preserva também
os valores anterior e atual de cada campo, além de estado, cidade e endereço
para contexto.

**Sair do estoque não significa venda.** A Caixa não informa a causa da
remoção naquela lista. Pode haver venda, suspensão, correção ou outra mudança
da fonte; a camada registra somente a diferença observável.

## Reproduzir

```bash
uv run python src/compare_snapshots.py \
  imoveis_geocoded_2026-09-01.parquet \
  imoveis_geocoded_2026-09-02.parquet \
  --output mudancas_2026-09-01_2026-09-02.parquet
```

Não existe banco de eventos nem estado adicional para manter: se a lógica de
comparação evoluir, os eventos podem ser recalculados dos snapshots originais.
