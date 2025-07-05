-- models/staging/stg_imoveis.sql

with source_data as (
    {% set states = ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"] %}
    {% for state in states %}
    select
        "link",
        "endereco",
        "bairro",
        "descricao",
        "preco",
        "avaliacao",
        "desconto",
        "modalidade",
        "foto",
        "cidade",
        "estado"
    from {{ ref('imoveis_' ~ state) }} -- Changed source to ref
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select
    -- Identifiers
    link as property_id,

    -- Property Details
    endereco as address,
    bairro as neighborhood,
    cidade as city,
    estado as state,
    descricao as description,
    modalidade as sales_modality,
    foto as photo_url,

    -- Financials
    preco as price,
    avaliacao as appraised_value,
    desconto as discount_percentage

from source_data
where link is not null -- Basic data quality filter
and preco is not null
and avaliacao is not null
and cidade is not null
and estado is not null
