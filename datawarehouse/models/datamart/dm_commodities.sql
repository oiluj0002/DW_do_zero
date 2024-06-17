WITH commodities AS (
    SELECT
        data,
        ticker,
        valor_fechamento
    FROM
        {{ ref('stg_commodities') }}
),

movimentacao AS (
    SELECT
        data,
        ticker,
        acao,
        quantidade
    FROM
        {{ ref('stg_movimentacao_commodities') }}
),

joined AS (
    SELECT
        c.data,
        c.ticker,
        c.valor_fechamento,
        m.acao,
        m.quantidade,
        (m.quantidade * c.valor_fechamento) AS valor,
        (CASE 
            WHEN m.acao = 'sell' THEN (m.quantidade * c.valor_fechamento)
            ELSE -(m.quantidade * c.valor_fechamento)
        END) AS receita
    FROM
        commodities c
    INNER JOIN 
        movimentacao m ON c.data = m.data AND c.ticker = m.ticker
),

last_day AS (
    SELECT
        MAX(data) AS max_date
    FROM
        joined
),

filtered AS (
    SELECT
        *
    FROM
        joined
    WHERE
        data = (SELECT max_date FROM last_day)
)

SELECT * FROM filtered