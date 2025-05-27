with 
troves as (
    select 
        date_trunc('day', block_time) as day,
        collateral_type,
        max_by(collateral_balance, (block_number, tx_index)) as balance
        
    from 
    query_5190838
    group by 1, 2
),
missing_days as (
    select 
        *,
        lead(day, 1, current_timestamp) over (partition by collateral_type order by day asc) as next_day
    from 
    troves
),
time_seq AS (
    select
        sequence(
        CAST('2025-01-01' as timestamp),
        date_trunc('day', cast(now() as timestamp)),
        interval '1' day
        ) as time 
),
days AS (
    select
        time.time as day 
    from time_seq
    cross join unnest(time) as time(time)
),
active_pools as (
    select 
        day,
        collateral_type,
        sum(balance) as balance
    from (
    select
        d.day,
        c.collateral_type,
        c.balance
    from 
    missing_days c
    inner join  
    days d 
        on c.day <= d.day 
        and d.day < c.next_day
    ) 
    where balance > 0 
    group by 1, 2 
),
ethereum_prices as (
    select 
        date_trunc('day', minute) as day,
        max_by(price, minute) as price 
    from 
    prices.usd 
    where minute >= date '2024-11-01'
    and symbol = 'WETH'
    and blockchain = 'ethereum'
    group by 1 
),
symbol_mapping as (
    select collateral_symbol, price_symbol from (values
        ('scrvUSD', 'SCRVUSD'),
        ('sfrxUSD', 'sfrxUSD_HARDCODED'),
        ('sDAI', 'SDAI'), 
        ('sUSDe', 'sUSDe'),
        ('sUSDS', 'sUSDS'),
        ('tBTC', 'TBTC'),
        ('wBTC', 'WBTC'),
        ('cbBTC', 'cbBTC')
    ) as t(collateral_symbol, price_symbol)
),
col_prices as (
    select 
        date_trunc('day', minute) as day,
        symbol,
        max_by(price, minute) as price 
    from 
    prices.usd
    where minute >= date '2024-11-01'
    and symbol in ('SCRVUSD', 'SDAI', 'sUSDe', 'sUSDS', 'TBTC', 'WBTC', 'cbBTC')
    and blockchain = 'ethereum'
    group by 1, 2 
    
    UNION ALL
    
    SELECT 
        time.time AS day,
        'sfrxUSD_HARDCODED' AS symbol,
        1.13 AS price
    FROM time_seq
    CROSS JOIN unnest(time) AS time(time)
),
eth_rates as (
    select 
        cp.*,
        cp.price/ep.price as eth_rate 
    from 
    col_prices cp 
    inner join 
    ethereum_prices ep 
        on cp.day = ep.day
),
enrich_tvl as (
    select 
        day,
        sum(balance_usd) as tvl_usd,
        sum(balance_eth) as tvl_eth 
    from (
    select 
        ap.*,
        ap.balance * er.price as balance_usd,
        ap.balance * er.eth_rate as balance_eth 
    from 
    active_pools ap 
    inner join 
    symbol_mapping sm 
        on ap.collateral_type = sm.collateral_symbol
    inner join 
    eth_rates er 
        on ap.day = er.day 
        and sm.price_symbol = er.symbol
    ) 
    group by 1 
),
bold_tvl as (
    select 
        day,
        sum(token_balance) as bold_tvl 
    from 
    query_5186181
    where address in (0x0b656b3af27e3a9cf143d16ed46466e0be27fecc, 0xd95692af0a30d936287bc7dc3837d3fbf7415f8a, 0x6f35f38d93165b67edc6abcd4b8ac5fef5ea86e0, 0x001fdd4f3405f97ed61c7dc817208dfeb8f6cb70, 0x38b5c7a506fff3d3dafd2d013e969d6e99cd9b73, 0x76365e44314c048a924314c3bd9bf59d6fa9e243, 0xe9a258f362fc7f8003a39b087046f64815cc9c56, 0x7f5d15f4053f1e34025907f0741f2abc4353c65c)
    group by 1 
)
select 
    et.*,
    bt.bold_tvl,
    (et.tvl_usd + bt.bold_tvl)/1e6 as total_usd
from 
enrich_tvl et 
inner join 
bold_tvl bt 
    on et.day = bt.day 
order by et.day desc