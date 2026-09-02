INSTALL httpfs;
LOAD httpfs;
INSTALL json;
LOAD json;

-- O manifesto aponta para o último retrato efetivamente publicado. Nada aqui
-- depende do relógio nem do fuso de quem consulta, e nada precisa ser
-- regerado — nem na virada do ano.
SET VARIABLE imoveis_caixa_snapshot = (
    SELECT parquet_url FROM read_json_auto('https://archive.org/download/imoveis-caixa-economica-federal/latest.json')
);

CREATE OR REPLACE VIEW imoveis_caixa AS
SELECT * FROM read_parquet(getvariable('imoveis_caixa_snapshot'));
