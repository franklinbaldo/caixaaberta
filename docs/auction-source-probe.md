# Probe: Baliza + CausaGanha como fontes de leilões

## Pergunta

Os acervos que Baliza e CausaGanha já publicaram no Internet Archive contêm informação suficiente para o Caixa Aberta descobrir leilões reais sem reconstruir a captura dessas fontes?

## Janela

A execução automatizada consulta os últimos 62 dias em relação à data da execução. Para esta PR, isso corresponde aproximadamente aos dois meses mais recentes.

## Método

O probe é deliberadamente consumidor dos artefatos públicos já produzidos pelos outros projetos.

### Baliza

1. Descobre os itens mensais `baliza-pncp-YYYY-MM` no Internet Archive.
2. Lê a metadata do item e seleciona Parquets de publicações/contratações/editais (com fallback para todos os Parquets do item).
3. Baixa os Parquets para armazenamento temporário.
4. Usa DuckDB localmente para pesquisar todas as colunas textuais por sinais de leilão.

### CausaGanha

1. Baixa `causaganha-catalog/manifest.parquet`, o índice canônico do corpus arquivado.
2. Seleciona Parquets da tabela `comunicacoes` cuja data esteja dentro da janela.
3. Para limitar custo de rede nesta primeira probe, baixa no máximo 20 Parquets recentes; o relatório registra separadamente quantos Parquets candidatos existem e quantos foram efetivamente examinados.
4. Usa DuckDB localmente para pesquisar todas as colunas textuais.

### Vocabulário inicial

`leilão`, `leilao`, `hasta pública`, `hasta publica`, `praça`, `praca`, `arrematação`, `arrematacao`, `leiloeiro`, `alienação judicial`, `alienacao judicial`.

Esse vocabulário é para descoberta, não é ainda um classificador. Falsos positivos são esperados e fazem parte do que esta probe pretende medir.

## Reprodutibilidade

```bash
uv run scripts/probe_auction_sources.py --days 62 --causa-max-files 20 --output probe-results.json
```

O script usa PEP 723 e só depende de `duckdb` e `httpx`.

## Resultado da execução desta PR

A preencher com a execução real do workflow `Auction source probe`. O JSON integral fica anexado ao workflow como `auction-source-probe-results`; depois da primeira execução, esta seção será atualizada com contagens, exemplos concretos e conclusão sobre viabilidade de cada fonte.

## Critério de sucesso

A probe é positiva se encontrarmos, nos artefatos reais já arquivados:

- no Baliza, registros que representem alienações/leilões administrativos reais e tragam identidade/proveniência suficiente para virar `auction`/`lot`;
- no CausaGanha, comunicações judiciais reais de leilão com número de processo e texto/evidência suficiente para posterior extração de datas, bens, valores e leiloeiro.

O objetivo não é provar cobertura nacional ou recall nesta PR. É provar o caminho de dados e medir uma amostra real antes de definir o contrato OKF definitivo.
