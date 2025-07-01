-- models/staging/stg_imoveis.sql

-- This model unnions all data from the individual state seed files.
-- It creates a single source of truth for all property listings before further transformations.

WITH all_sources AS (
    SELECT * FROM {{ source('seeds', 'imoveis_AC') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_AL') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_AM') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_AP') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_BA') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_CE') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_DF') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_ES') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_GO') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_MA') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_MG') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_MS') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_MT') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_PA') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_PB') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_PE') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_PI') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_PR') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_RJ') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_RN') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_RO') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_RR') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_RS') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_SC') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_SE') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_SP') }} UNION ALL
    SELECT * FROM {{ source('seeds', 'imoveis_TO') }}
)

SELECT
    -- Cast columns to appropriate types if necessary, though dbt seed configurations
    -- in dbt_project.yml should ideally handle initial typing.
    -- For example, ensuring numeric types are correct.
    CAST(link AS VARCHAR) AS link, -- Assuming link is a string identifier
    CAST(endereco AS VARCHAR) AS endereco,
    CAST(bairro AS VARCHAR) AS bairro,
    CAST(descricao AS VARCHAR) AS descricao,
    CAST(preco AS DECIMAL(18, 2)) AS preco, -- Example: Numeric with precision
    CAST(avaliacao AS DECIMAL(18, 2)) AS avaliacao, -- Example: Numeric with precision
    CAST(desconto AS VARCHAR) AS desconto, -- Or FLOAT if it's a numeric percentage like 0.10
    CAST(modalidade AS VARCHAR) AS modalidade,
    CAST(foto AS VARCHAR) AS foto, -- Or BOOLEAN if it's a true/false flag
    CAST(cidade AS VARCHAR) AS cidade,
    CAST(estado AS VARCHAR) AS estado,

    -- Add a source file column for traceability, though dbt lineage will also track this
    -- This is more for direct querying convenience if needed.
    -- Note: This static assignment is not robust if using `dbt run --models stg_imoveis` directly
    -- without running seeds first, or if a seed file is empty.
    -- A more dbt-native way to get source is `{{ this.resolve_path() }}` but that's for the model itself.
    -- For seeds, dbt automatically creates relations like `raw_data.imoveis_AC`.
    -- The `source()` macro abstracts the schema.
    -- Let's assume for now the UNION ALL structure is sufficient and no explicit source file column here.

    -- Add ingestion timestamp
    CURRENT_TIMESTAMP AS ingestion_timestamp

FROM all_sources
-- Optional: Add a WHERE clause to filter out any completely empty rows if necessary,
-- though cleaning should ideally happen in the seeds or earlier.
-- WHERE link IS NOT NULL OR endereco IS NOT NULL -- etc.

-- Note on `source` vs `ref`:
-- `source` is used here because we are selecting from seed files, which are defined as sources in a .yml file.
-- If these were other dbt models, we would use `ref('model_name')`.
-- The dbt_project.yml should have a `seed-paths` config, and seeds need to be loaded via `dbt seed`.
-- The schema.yml for seeds (e.g., `dbt_real_estate/seeds/schema.yml`) defines these sources.
-- Make sure the source names match what's in your seeds schema.yml.
-- Example for seeds schema.yml:
-- sources:
--   - name: seeds # This is a convention, can be any name
--     schema: "{{ target.schema }}" # Or your raw data schema
--     tables:
--       - name: imoveis_AC
--       - name: imoveis_AL
--       ... and so on for all states.
-- The `source()` macro then takes two arguments: `source('seeds_group_name', 'table_name')`.
-- If your seeds are directly in the target schema without a group, you might need to adjust.
-- However, the standard is that `dbt seed` loads them, and they become accessible via `ref`
-- if you treat them as "models" of type seed, OR via `source` if you declare them as sources.
-- For simplicity and common practice, `ref` is often used directly on seed names
-- *after* `dbt seed` has run, as dbt creates relations for them.
-- Let's switch to `ref` as it's more common for already seeded data.

-- Corrected approach using ref, assuming `dbt seed` has been run:
-- models/staging/stg_imoveis.sql
WITH all_sources_ref AS (
    SELECT * FROM {{ ref('imoveis_AC') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_AL') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_AM') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_AP') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_BA') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_CE') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_DF') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_ES') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_GO') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_MA') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_MG') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_MS') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_MT') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_PA') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_PB') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_PE') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_PI') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_PR') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_RJ') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_RN') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_RO') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_RR') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_RS') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_SC') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_SE') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_SP') }} UNION ALL
    SELECT * FROM {{ ref('imoveis_TO') }}
)

SELECT
    CAST(link AS VARCHAR) AS link,
    CAST(endereco AS VARCHAR) AS endereco,
    CAST(bairro AS VARCHAR) AS bairro,
    CAST(descricao AS VARCHAR) AS descricao,
    CAST(preco AS DECIMAL(18, 2)) AS preco,
    CAST(avaliacao AS DECIMAL(18, 2)) AS avaliacao,
    CAST(desconto AS VARCHAR) AS desconto, -- Keep as VARCHAR if it contains '%' or other symbols not purely numeric
    CAST(modalidade AS VARCHAR) AS modalidade,
    CAST(foto AS VARCHAR) AS foto, -- Keep as VARCHAR, could be URL or indicator
    CAST(cidade AS VARCHAR) AS cidade,
    CAST(estado AS VARCHAR(2)) AS estado, -- Assuming 2-char state codes

    CURRENT_TIMESTAMP AS ingestion_timestamp

FROM all_sources_ref
WHERE link IS NOT NULL -- Basic data quality filter: ensure listings have a link
AND estado IS NOT NULL -- Ensure state is present for partitioning or filtering
AND cidade IS NOT NULL -- Ensure city is present

-- Further data quality checks or transformations can be added here or in downstream models.
-- Example: Standardize 'bairro' to uppercase
-- TRIM(UPPER(CAST(bairro AS VARCHAR))) AS bairro,

-- This staging model now provides a unified base for all properties.
-- Next step would be to use this in the Python geocoding model.
