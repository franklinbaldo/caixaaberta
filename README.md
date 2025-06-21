# Caixa Aberta Simplificado

Este projeto coleta e processa dados de imóveis do portal da Caixa Econômica Federal.

## Funcionalidades
- Baixa dados de imóveis por estado.
- Consolida os dados em um arquivo CSV principal (`imoveis_BR.csv`).
- Opcionalmente, enriquece os dados com coordenadas geográficas (latitude/longitude) usando Nominatim.
- Mantém um cache SQLite (`cache.sqlite`) para geocodificação.

## Como Rodar
1. Clone o repositório.
2. Crie um ambiente virtual e instale as dependências:
   `pip install -r requirements.txt`
3. Crie um arquivo `.env` a partir do `.env.sample` e preencha `URL_BASE` (geralmente já vem) e `GEOCODER_KEY` (seu e-mail para User-Agent do Nominatim).
4. Execute o pipeline:
   - Para baixar, consolidar e geocodificar: `python src/pipeline.py --geo`
   - Para apenas baixar e consolidar: `python src/pipeline.py`
   - Para pular o download e apenas consolidar (e opcionalmente geocodificar) dados existentes na pasta `data/`: `python src/pipeline.py --skip-download [--geo]`

## Dependências Principais
- pandas
- requests
- lxml
- python-dotenv
- geopy
