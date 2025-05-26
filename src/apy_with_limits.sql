WITH 
interest_rewards AS (
    SELECT 
        CASE 
            WHEN to = 0x7f5d15f4053f1e34025907f0741f2abc4353c65c THEN 'cbBTC'
            WHEN to = 0xe9a258f362fc7f8003a39b087046f64815cc9c56 THEN 'wBTC'
            WHEN to = 0x76365e44314c048a924314c3bd9bf59d6fa9e243 THEN 'tBTC'
            WHEN to = 0x38b5c7a506fff3d3dafd2d013e969d6e99cd9b73 THEN 'sUSDe'
            WHEN to = 0x001fdd4f3405f97ed61c7dc817208dfeb8f6cb70 THEN 'sfrxUSD'
            WHEN to = 0x6f35f38d93165b67edc6abcd4b8ac5fef5ea86e0 THEN 'sUSDS'
            WHEN to = 0xd95692af0a30d936287bc7dc3837d3fbf7415f8a THEN 'sDAI'
            WHEN to = 0x0b656b3af27e3a9cf143d16ed46466e0be27fecc THEN 'scrvUSD'
        END AS collateral_type,
        date_trunc('day', evt_block_time) AS day, 
        SUM(value/1e18) AS usdaf_amount 
    FROM 
    asymmetry_finance_ethereum.usdaf_v1_evt_transfer
    WHERE to IN (0x0b656b3af27e3a9cf143d16ed46466e0be27fecc, 0xd95692af0a30d936287bc7dc3837d3fbf7415f8a, 0x6f35f38d93165b67edc6abcd4b8ac5fef5ea86e0, 0x001fdd4f3405f97ed61c7dc817208dfeb8f6cb70, 0x38b5c7a506fff3d3dafd2d013e969d6e99cd9b73, 0x76365e44314c048a924314c3bd9bf59d6fa9e243, 0x7f5d15f4053f1e34025907f0741f2abc4353c65c )
    AND "from" = 0x0000000000000000000000000000000000000000
    GROUP BY 1, 2 
),
liquidation_rewards AS (
    SELECT 
        collateral_type,
        date_trunc('day', block_time) AS day,
        SUM(collateral_sent_sp) AS liquidation_rewards
    FROM 
    query_5186144
    GROUP BY 1, 2 
),
time_seq AS (
    SELECT
        sequence(
        CAST('2025-01-01' AS timestamp),
        date_trunc('day', CAST(now() AS timestamp)),
        interval '1' day
        ) AS time 
),
days AS (
    SELECT
        time.time AS day 
    FROM time_seq
    CROSS JOIN unnest(time) AS time(time)
),
collaterals AS (
    SELECT 
        collateral_type 
    FROM (
        VALUES 
            ('cbBTC'), ('wBTC'), ('tBTC'), ('sUSDe'), ('sfrxUSD'), ('sUSDS'), ('sDAI'), ('scrvUSD')
    ) AS tmp (collateral_type)
),
get_all_collaterals AS (
    SELECT 
        d.day,
        c.collateral_type
    FROM 
    days d 
    INNER JOIN 
    collaterals c 
        ON 1 = 1 
),
get_all_rewards AS (
    SELECT 
        ga.day,
        ga.collateral_type,
        COALESCE(ir.usdaf_amount, 0) AS usdaf_rewards,
        COALESCE(lr.liquidation_rewards, 0) AS liquidation_rewards
    FROM 
    get_all_collaterals ga 
    LEFT JOIN 
    interest_rewards ir 
        ON ga.day = ir.day 
        AND ga.collateral_type = ir.collateral_type 
    LEFT JOIN 
    liquidation_rewards lr 
        ON ga.day = lr.day 
        AND ga.collateral_type = lr.collateral_type 
),
symbol_mapping AS (
    SELECT collateral_symbol, price_symbol FROM (VALUES
        ('scrvUSD', 'CRVUSD'),
        ('sfrxUSD', 'CRVUSD'),
        ('sDAI', 'SDAI'), 
        ('sUSDe', 'SUSDE'),
        ('sUSDS', 'sUSDS'),
        ('tBTC', 'TBTC'),
        ('wBTC', 'WBTC'),
        ('cbBTC', 'cbBTC')
    ) AS t(collateral_symbol, price_symbol)
),
get_prices AS (
    SELECT 
        date_trunc('day', minute) AS day,
        symbol, 
        max_by(price, minute) AS price 
    FROM 
    prices.usd 
    WHERE minute >= date '2024-10-31'
    AND symbol IN ('CRVUSD', 'SDAI', 'SUSDE', 'sUSDS', 'TBTC', 'WBTC', 'cbBTC')
    AND blockchain = 'ethereum'
    GROUP BY 1, 2 
),
get_liquid_usd AS (
    SELECT 
        ga.day,
        ga.collateral_type,
        ga.usdaf_rewards,
        COALESCE(ga.liquidation_rewards * gp.price, 0) AS liquidation_rewards 
    FROM 
    get_all_rewards ga 
    LEFT JOIN 
    symbol_mapping sm 
        ON ga.collateral_type = sm.collateral_symbol
    LEFT JOIN 
    get_prices gp 
        ON ga.day = gp.day 
        AND sm.price_symbol = gp.symbol 
),
balances AS (
    SELECT 
        day,
        CASE 
            WHEN address = 0x7f5d15f4053f1e34025907f0741f2abc4353c65c THEN 'cbBTC'
            WHEN address = 0xe9a258f362fc7f8003a39b087046f64815cc9c56 THEN 'wBTC'
            WHEN address = 0x76365e44314c048a924314c3bd9bf59d6fa9e243 THEN 'tBTC'
            WHEN address = 0x38b5c7a506fff3d3dafd2d013e969d6e99cd9b73 THEN 'sUSDe'
            WHEN address = 0x001fdd4f3405f97ed61c7dc817208dfeb8f6cb70 THEN 'sfrxUSD'
            WHEN address = 0x6f35f38d93165b67edc6abcd4b8ac5fef5ea86e0 THEN 'sUSDS'
            WHEN address = 0xd95692af0a30d936287bc7dc3837d3fbf7415f8a THEN 'sDAI'
            WHEN address = 0x0b656b3af27e3a9cf143d16ed46466e0be27fecc THEN 'scrvUSD'
        END AS collateral_type,
        token_balance AS usdaf_supply
    FROM 
    query_5186181
    WHERE address IN (0x7f5d15f4053f1e34025907f0741f2abc4353c65c, 0xe9a258f362fc7f8003a39b087046f64815cc9c56, 0x76365e44314c048a924314c3bd9bf59d6fa9e243, 0x38b5c7a506fff3d3dafd2d013e969d6e99cd9b73, 0x001fdd4f3405f97ed61c7dc817208dfeb8f6cb70, 0x6f35f38d93165b67edc6abcd4b8ac5fef5ea86e0, 0xd95692af0a30d936287bc7dc3837d3fbf7415f8a, 0x0b656b3af27e3a9cf143d16ed46466e0be27fecc)
),
join_balances AS (
    SELECT 
        gl.day,
        gl.collateral_type,
        gl.usdaf_rewards AS total_rewards,
        b.usdaf_supply 
    FROM 
    get_liquid_usd gl 
    LEFT JOIN  -- Changed from INNER to LEFT JOIN
    balances b 
        ON gl.collateral_type = b.collateral_type 
        AND gl.day = b.day 
    WHERE b.usdaf_supply IS NOT NULL  -- Only include days with actual balance data
)
SELECT 
    day,
    collateral_type,
    total_rewards,
    usdaf_supply,
    rewards,
    avg_supply,
    -- Add minimum supply threshold to prevent extreme APRs
    CASE 
        WHEN avg_supply < 10 THEN NULL  -- Don't calculate APR for supplies < $10
        ELSE rewards/avg_supply
    END AS period_apy,
    CASE 
        WHEN avg_supply < 10 THEN NULL
        ELSE (rewards/avg_supply) * 365
    END AS apr,
    CASE 
        WHEN avg_supply < 10 THEN NULL
        ELSE (rewards/avg_supply) * 365 * 100
    END AS apr_two,
    -- Calculate total APR only for meaningful supplies
    avg(
        CASE 
            WHEN avg_supply >= 10 THEN ((rewards/avg_supply)) * 365 * 100
            ELSE NULL
        END
    ) OVER (PARTITION BY day) AS total_apr
FROM (
SELECT 
    day,
    collateral_type,
    total_rewards,
    usdaf_supply,
    SUM(total_rewards) OVER (PARTITION BY collateral_type ORDER BY day ROWS BETWEEN 0 PRECEDING AND CURRENT ROW) AS rewards,
    AVG(usdaf_supply) OVER (PARTITION BY collateral_type ORDER BY day ROWS BETWEEN 0 PRECEDING AND CURRENT ROW) AS avg_supply 
FROM 
join_balances
WHERE day != current_date 
) 
WHERE day >= (CASE 
                WHEN '{{Time Period}}' = 'All Time' THEN date '2024-11-01'
                WHEN '{{Time Period}}' = '1 Year' THEN date_trunc('day', now() - interval '1' year)
                WHEN '{{Time Period}}' = '6 Months' THEN date_trunc('day', now() - interval '6' month)
                WHEN '{{Time Period}}' = '3 Months' THEN date_trunc('day', now() - interval '3' month)
                WHEN '{{Time Period}}' = '1 Month' THEN date_trunc('day', now() - interval '1' month)
            END)
ORDER BY 1 DESC, lower(collateral_type) DESC