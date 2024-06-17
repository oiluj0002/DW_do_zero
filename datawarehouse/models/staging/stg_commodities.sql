WITH source AS (
    SELECT
        "Date",
        "Close",
        "symbol"
    FROM
        {{ source ('dbsales', 'commodities') }}
),

renamed AS (

    SELECT
        CAST("Date" AS DATE) AS data,
        "Close" AS valor_fechamento,
        "symbol" AS ticker
    FROM
        source
)

SELECT * FROM renamed