INSTALL httpfs;
LOAD httpfs;

-- A view lê o retrato do dia corrente. O nome do arquivo e o item do ano são
-- calculados a partir da data: nada aqui precisa ser atualizado.
CREATE OR REPLACE VIEW imoveis_caixa AS
SELECT * FROM read_parquet(
    'https://archive.org/download/imoveis-caixa-economica-federal-'
    || strftime(current_date, '%Y')
    || '/imoveis_geocoded_'
    || strftime(current_date, '%Y-%m-%d')
    || '.parquet'
);
