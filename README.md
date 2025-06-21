# Caixa Aberta

Pipeline ETL → CSV de imóveis leiloados pela Caixa.

## Uso local
```bash
# Crie e ative um ambiente virtual (ex: .venv)
uv venv
source .venv/bin/activate # ou .venv\Scripts\activate no Windows

# Instale as dependências (incluindo o próprio pacote caixaaberta em modo editável)
uv pip install -e .

# Crie .env a partir de .env.sample e adicione sua GEOCODER_KEY (e-mail para Nominatim)
# cp .env.sample .env
# nano .env

# Execute o pipeline (roda o módulo src/caixaaberta/pipeline.py)
python -m caixaaberta.pipeline --geo
```
Flags opcionais para `pipeline.py`:
* `--skip-download`: Pula o download de CSVs dos estados (usa dados locais da pasta `data/`).
* `--geo`: Ativa a geocodificação (requer `GEOCODER_KEY` no `.env`).

## CI
Ver `.github/workflows/ci.yml`. Runner GitHub = infra zero.

O cache da geocodificação é salvo em `data/cache.sqlite`.
Os dados consolidados são salvos em `imoveis_BR.csv`.
