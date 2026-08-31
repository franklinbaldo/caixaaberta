INSTALL httpfs;
LOAD httpfs;

CREATE OR REPLACE VIEW imoveis_caixa AS
SELECT * FROM read_parquet('https://archive.org/download/imoveis-caixa-economica-federal/imoveis_geocoded.parquet');
