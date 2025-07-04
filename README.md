# Real Estate Data Pipeline (Caixa Aberta Simplificado - dbt Version)

Este projeto ingere dados de imóveis da Caixa Econômica Federal (previamente baixados como CSVs), os processa utilizando dbt e DuckDB, e os arquiva no Archive.org.

## Funcionalidades Principais

- **Ingestão de Dados com dbt:** Carrega dados de imóveis de arquivos CSV (localizados em `data/`) para um banco de dados DuckDB (`dbt_real_estate/real_estate_data.db`) usando dbt seeds.
- **Estrutura de Dados Organizada:** Cada CSV de estado (e.g., `imoveis_AC.csv`) é carregado em uma tabela correspondente no DuckDB.
- **Arquivamento no Archive.org:** Faz o upload do banco de dados DuckDB gerado para o Archive.org.
- **Testes de Dados:** Inclui testes de dbt para garantir a integridade dos dados carregados (e.g., unicidade de IDs, valores não nulos).
- **Testes Unitários:** Testes Pytest para o script de upload para o Archive.org.
- **Automação com GitHub Actions:** Workflow para construir, testar e fazer upload dos dados automaticamente.
- **Busca de Dados Automatizada:** Script para baixar os dados mais recentes de imóveis da Caixa.
- **Geocodificação Integrada:** Geocodificação de endereços como parte do pipeline dbt.

_(Nota: O script legado `src/pipeline.py` foi removido. Suas funcionalidades de download de dados e geocodificação foram modernizadas e integradas ao novo fluxo de trabalho com `src/fetch_data.py` e modelos dbt Python, respectivamente.)_

## Pré-requisitos

- Python 3.8+
- `uv` (gerenciador de pacotes e ambientes virtuais Python). Instruções de instalação: [Astral uv](https://github.com/astral-sh/uv).

## Configuração Local

1.  **Clone o repositório:**

    ```bash
    git clone <URL_DO_REPOSITORIO>
    cd <NOME_DO_REPOSITORIO>
    ```

2.  **Crie um ambiente virtual e instale as dependências:**
    Recomenda-se usar `uv` para criar o ambiente e instalar as dependências definidas em `pyproject.toml`.

    ```bash
    uv venv .venv # Cria um ambiente virtual chamado .venv
    # Ative o ambiente (Unix/macOS)
    # source .venv/bin/activate
    # Ative o ambiente (Windows PowerShell)
    # .\.venv\Scripts\Activate.ps1
    uv pip install -e .[dev] # Instala o projeto em modo editável com dependências de desenvolvimento
    ```

3.  **Variáveis de Ambiente (Opcional para execução local, necessário para upload):**
    Copie `.env.sample` para `.env` e configure as variáveis, se necessário.
    ```bash
    cp .env.sample .env
    ```

    - `IA_ACCESS_KEY` e `IA_SECRET_KEY`: Necessárias para fazer upload para o Archive.org. Deixe em branco se for apenas testar localmente com `--upload-dry-run`.
    - `URL_BASE`: Utilizada pelo script `src/fetch_data.py` para baixar os dados da Caixa. Exemplo: `https://venda-imoveis.caixa.gov.br/listaweb/Lista_imoveis_{}.htm`
    - `GEOCODER_KEY`: Chave de API para o serviço de geocodificação (ex: Nominatim, Google Geocoding API). Necessária para o modelo dbt `imoveis_geocoded.py`.

## Como Rodar o Pipeline Localmente

O pipeline principal é executado através do script `src/run_dbt_pipeline.py`. Certifique-se de que o ambiente virtual (`.venv`) está ativo ou prefixe os comandos Python com `.venv/bin/python` (ou `.\.venv\Scripts\python` no Windows).

1.  **Executar o pipeline completo (dbt build + upload):**

    ```bash
    python src/run_dbt_pipeline.py
    ```

    Para fazer um dry-run do upload (sem enviar dados reais para o Archive.org):

    ```bash
    python src/run_dbt_pipeline.py --upload-dry-run
    ```

2.  **Opções do script `run_dbt_pipeline.py`:**
    Use `python src/run_dbt_pipeline.py --help` para ver todas as opções. Inclui:
    - `--skip-dbt-build`: Pula a etapa `dbt build`.
    - `--skip-upload`: Pula a etapa de upload para o Archive.org.
    - `--upload-dry-run`: Simula o upload para o Archive.org.
    - `--archive-item-identifier <ID>`: Especifica um identificador para o item no Archive.org.
    - `--archive-item-title <TITLE>`: Especifica um título para o item no Archive.org.
    - `--archive-item-description <DESC>`: Especifica uma descrição para o item no Archive.org.

## Como Rodar Testes Localmente

1.  **Rodar testes dbt:**
    Os testes dbt verificam a qualidade dos dados carregados nos seeds.

    ```bash
    # A partir da raiz do projeto (com .venv ativo ou prefixando dbt)
    .venv/bin/dbt test --project-dir ./dbt_real_estate --profiles-dir ./dbt_real_estate
    ```

2.  **Rodar testes Pytest:**
    Os testes Pytest verificam a funcionalidade do script de upload.
    ```bash
    # A partir da raiz do projeto (com .venv ativo ou prefixando python)
    .venv/bin/python -m pytest -v tests/
    ```

## Estrutura do Projeto

- `src/`: Contém os scripts Python principais.
  - `fetch_data.py`: Script para baixar os dados de imóveis da Caixa e salvá-los em `dbt_real_estate/seeds/`.
  - `run_dbt_pipeline.py`: Orquestra o build do dbt e o upload para o Archive.org.
  - `upload_to_archive.py`: Script para fazer upload do banco DuckDB para o Archive.org.
  - `geocoding_utils.py`: Utilitários para geocodificação de endereços.
  - `reporter.py`: Script para gerar um resumo dos dados processados.
- `dbt_real_estate/`: Projeto dbt.
  - `seeds/`: Contém os arquivos CSV de entrada (baixados por `fetch_data.py`) e seus `schema.yml` com testes.
  - `models/`: Contém os modelos dbt.
    - `staging/stg_imoveis.sql`: Modelo que unifica todos os dados dos seeds.
    - `marts/imoveis_geocoded.py`: Modelo Python que geocodifica os dados de `stg_imoveis`.
  - `target/`: Gerado pelo dbt, contém artefatos de compilação e o banco DuckDB.
  - `profiles.yml`: Configuração de conexão do dbt para o DuckDB.
  - `dbt_project.yml`: Configuração principal do projeto dbt.
- `data/`: Diretório onde os CSVs originais dos imóveis são armazenados. Estes são copiados para `dbt_real_estate/seeds/` pelo workflow.
- `tests/`: Contém os testes Pytest.
- `.github/workflows/`: Contém os workflows do GitHub Actions.
  - `main.yml`: Workflow principal para CI/CD, incluindo testes e deploy para Archive.org.
- `pyproject.toml`: Define as dependências do projeto e configurações de build/ferramentas.
- `.env.sample`: Exemplo de arquivo de variáveis de ambiente.

## Automação (CI/CD)

O projeto utiliza GitHub Actions para automação, definido em `.github/workflows/main.yml`. O workflow:

- É disparado em pushes para a branch `main` ou manualmente (`workflow_dispatch`).
- Configura Python e `uv`.
- Instala dependências.
- Executa `src/fetch_data.py` para baixar os dados mais recentes.
- Roda testes dbt (`dbt test`).
- Roda testes Pytest.
- Executa `src/run_dbt_pipeline.py` (que internamente roda `dbt build`) para processar os dados e construir o banco de dados DuckDB.
- Faz o upload do banco de dados para o Archive.org.
- **Importante:** As credenciais `IA_ACCESS_KEY`, `IA_SECRET_KEY` e `GEOCODER_KEY` devem ser configuradas como segredos no repositório GitHub para que o workflow funcione corretamente.

## Dependências Principais (gerenciadas via `pyproject.toml`)

- `duckdb`: Banco de dados analítico em-processo.
- `dbt-core`, `dbt-duckdb`: Ferramenta de transformação de dados e adaptador para DuckDB.
- `internetarchive`: Biblioteca para interagir com o Archive.org.
- `pandas`: Para manipulação de dados (usado nos scripts Python e implicitamente pelo dbt-duckdb com seeds).
- `pytest`, `pytest-mock`: Para testes unitários.
- `uv`: Para gerenciamento de pacotes e ambiente.
- `python-dotenv`: Para carregar variáveis de ambiente de arquivos `.env` (útil para desenvolvimento local).
- `requests`: Para fazer requisições HTTP (usado por `src/fetch_data.py`).
- `lxml`: Para fazer parse de HTML (usado por `src/fetch_data.py`).
- `geopy`: Para geocodificação (usado por `src/geocoding_utils.py`).
