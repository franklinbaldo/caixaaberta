# Relatório de Inteligência - caixaaberta

## O que é este repo
**Propósito:** Este projeto, "Caixa Aberta Simplificado", é um pipeline de dados para ingerir dados de imóveis da Caixa Econômica Federal a partir de arquivos CSV, processá-los, estruturá-los, e enviá-los para o Archive.org.
**Linguagem:** Python e SQL (dbt).
**Stack:**
- **Transformação de Dados:** dbt (dbt-core, dbt-duckdb)
- **Banco de Dados/Engine:** DuckDB
- **Ambiente/Gerenciamento:** `uv` (para ambientes virtuais e dependências)
- **CI/CD:** GitHub Actions
- **Upload:** `internetarchive` API (Archive.org)
**Dependências principais:** `duckdb`, `dbt-core`, `dbt-duckdb`, `pandas`, `internetarchive`, `requests`, `lxml`, `geopy`, `pytest` (para testes).

## Estado atual
**Issues abertas:**
- #34 - feat: Complete TODO items and update pipeline
- #26 - feat: Add unit tests and GitHub Actions CI

**PRs abertos:**
- #34 - feat: Complete TODO items and update pipeline (Status: open, Draft: false)
- #26 - feat: Add unit tests and GitHub Actions CI (Status: open, Draft: false)

**Status CI:**
- Falhando (Failure). A última execução no GitHub Actions resultou em falha (status: completed, conclusion: failure).

## Posição estratégica
**Recomendação:** **Core**
**Justificativa:** O repositório é um pipeline completo e automatizado que lida com extração, transformação de dados (usando dbt e DuckDB) e carga para o Archive.org. Existem workflows de CI implementados, arquivos `.github/workflows` ativos e testes no código, indicando que trata-se de um projeto estruturado de dados que precisa de manutenção e expansão. Portanto, não é apenas experimental e possui um papel central em sua funcionalidade proposta.

## Próximas 3 ações Jules recomendadas
1. **Corrigir Testes e CI (Prioridade Alta):** Corrigir os erros de sintaxe e dependências nos arquivos de teste (e.g. `SyntaxError` em `test_fetch_data.py`, erros de importação em `test_geocoding_utils.py` e `test_upload_to_archive.py`) para que os testes passem e o pipeline de CI (que está falhando atualmente) volte a funcionar corretamente (relacionado à PR/Issue #26).
2. **Integrar Busca Automatizada de Dados (Prioridade Alta):** Criar `src/fetch_data.py` (migrando lógica de download do `src/pipeline.py`) para salvar os CSVs diretamente em `dbt_real_estate/seeds/` e atualizar o workflow do GitHub Actions para rodar este script antes do `dbt build`, completando o item 1 do `TODO.md` (relacionado à PR/Issue #34).
3. **Reintroduzir Geocodificação no dbt (Prioridade Média):** Criar o modelo de staging `stg_imoveis.sql` e o modelo dbt Python `imoveis_geocoded.py` na pasta `marts` para aplicar geocodificação usando `geocoding_utils.py`, conforme sugerido no item 2 do `TODO.md`.

## Issues prontas para implementação
- **#34 (Complete TODO items and update pipeline):** Baseado no `TODO.md`, os passos 1, 2, 3 e 4 estão muito bem descritos, com recortes de código (`fetch_data.py`, `imoveis_geocoded.py`, `reporter.py`) demonstrando exatamente o que precisa ser feito para implementar a extração e a geocodificação.
- **#26 (Add unit tests and GitHub Actions CI):** Parte disso parece já estar presente em `tests/`, mas os testes estão falhando devido a imports e sintaxe no Python 3.12. A issue em si tem propósito claro: consolidar os testes e garantir que o CI passe na branch principal.