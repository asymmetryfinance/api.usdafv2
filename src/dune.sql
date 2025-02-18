with 

interest_rewards as (
    select 
        case 
            when to = 0x08c008b799e81f50b918b331c6d632db45d4c704 then 'scrvUSD'
            when to = 0x30d94f227e409b84ce39aa2fa48f251dcc0896a7 then 'sDAI'
            when to = 0xe01c600c5cf4d24ad88429d86f5db4eec0c99509 then 'sfrxETH'
            when to = 0xcd0e01c140413663f452a055ebb086ccfd3718ae then 'tBTC'
            when to = 0xb4992903e80058dcf2dc015a32a147dba8b9c7d1 then 'wBTC'
            when to = 0x1bcbf58cae63800681828425c3b7fe80b5534907 then 'sUSDS'
        end as collateral_type,
        date_trunc('day', evt_block_time) as day, 
        sum(value/1e18) as bold_amount 
    from 
    asymmetry_finance_ethereum.usadv1_evt_transfer
    where to in (0x08c008b799e81f50b918b331c6d632db45d4c704, 0x30d94f227e409b84ce39aa2fa48f251dcc0896a7, 0xe01c600c5cf4d24ad88429d86f5db4eec0c99509, 0xcd0e01c140413663f452a055ebb086ccfd3718ae, 0xb4992903e80058dcf2dc015a32a147dba8b9c7d1, 0x1bcbf58cae63800681828425c3b7fe80b5534907)
    and "from" = 0x0000000000000000000000000000000000000000
    group by 1, 2 
),

liquidation_rewards as (
    select 
        collateral_type,
        date_trunc('day', block_time) as day,
        sum(collateral_sent_sp) as liquidation_rewards
    from 
    query_4742438
    group by 1, 2 
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

collaterals as (
    select 
        collateral_type 
    from (
        values 
            ('scrvUSD'), ('sDAI'), ('sfrxETH'), ('tBTC'), ('wBTC'), ('sUSDS')

    ) as tmp (collateral_type)
),

get_all_collaterals as (
    select 
        d.day,
        c.collateral_type
    from 
    days d 
    inner join 
    collaterals c 
        on 1 = 1 
),

get_all_rewards as (
    select 
        ga.day,
        ga.collateral_type,
        coalesce(ir.bold_amount, 0) as bold_rewards,
        coalesce(0, 0) as liquidation_rewards
    from 
    get_all_collaterals ga 
    left join 
    interest_rewards ir 
        on ga.day = ir.day 
        and ga.collateral_type = ir.collateral_type 
    left join 
    liquidation_rewards lr 
        on ga.day = lr.day 
        and ga.collateral_type = lr.collateral_type 
),

get_prices as (
    select 
        date_trunc('day', minute) as day,
        symbol, 
        max_by(price, minute) as price 
    from 
    prices.usd 
    where minute >= date '2024-10-31'
    and symbol in ('scrvUSD', 'sDAI', 'sfrxETH', 'tBTC', 'wBTC', 'sUSDS')
    and blockchain = 'ethereum'
    group by 1, 2 
),

get_liquid_usd as (
    select 
        ga.day,
        ga.collateral_type,
        ga.bold_rewards,
        coalesce(ga.liquidation_rewards * gp.price, 0) as liquidation_rewards 
    from 
    get_all_rewards ga 
    left join 
    get_prices gp 
        on ga.day = gp.day 
        and ga.collateral_type = gp.symbol 
),

balances as (
    select 
        day,
        case 
            when address = 0x08c008b799e81f50b918b331c6d632db45d4c704 then 'scrvUSD'
            when address = 0x30d94f227e409b84ce39aa2fa48f251dcc0896a7 then 'sDAI'
            when address = 0xe01c600c5cf4d24ad88429d86f5db4eec0c99509 then 'sfrxETH'
            when address = 0xcd0e01c140413663f452a055ebb086ccfd3718ae then 'tBTC'
            when address = 0xb4992903e80058dcf2dc015a32a147dba8b9c7d1 then 'wBTC'
            when address = 0x1bcbf58cae63800681828425c3b7fe80b5534907 then 'sUSDS'
        end as collateral_type,
        token_balance as bold_supply
    from 
    query_4742488
    where address in (0x08c008b799e81f50b918b331c6d632db45d4c704, 0x30d94f227e409b84ce39aa2fa48f251dcc0896a7, 0xe01c600c5cf4d24ad88429d86f5db4eec0c99509, 0xcd0e01c140413663f452a055ebb086ccfd3718ae, 0xb4992903e80058dcf2dc015a32a147dba8b9c7d1, 0x1bcbf58cae63800681828425c3b7fe80b5534907)
),

join_balances as (
    select 
        gl.day,
        gl.collateral_type,
        gl.bold_rewards as total_rewards, -- removed  + gl.liquidation_rewards --
        b.bold_supply 
    from 
    get_liquid_usd gl 
    inner join 
    balances b 
        on gl.collateral_type = b.collateral_type 
        and gl.day = b.day 
)

select 
    day,
    collateral_type,
    total_rewards,
    bold_supply,
    rewards,
    avg_supply,
    rewards/avg_supply as period_apy,
    ((rewards/avg_supply)/{{nDays Trailing  Num}}) * 365 as apr,
    ((rewards/avg_supply)/{{nDays Trailing  Num}}) * 365 * 100 as apr_two,
    avg(((rewards/avg_supply)/{{nDays Trailing  Num}}) * 365 * 100) over (partition by day) as total_apr
from (
select 
    day,
    collateral_type,
    total_rewards,
    bold_supply,
    sum(total_rewards) over (partition by collateral_type order by day rows between {{nDays Trailing  Num}} - 1 preceding and current row) as rewards,
    avg(bold_supply) over (partition by collateral_type order by day rows between {{nDays Trailing  Num}} - 1 preceding and current row) as avg_supply 
from 
join_balances
where day != current_date 
) 
where day >= (case 
                when '{{Time Period}}' = 'All Time' then date '2024-11-01'
                when '{{Time Period}}' = '1 Year' then date_trunc('day', now() - interval '1' year)
                when '{{Time Period}}' = '6 Months' then date_trunc('day', now() - interval '6' month)
                when '{{Time Period}}' = '3 Months' then date_trunc('day', now() - interval '3' month)
                when '{{Time Period}}' = '1 Month' then date_trunc('day', now() - interval '1' month)
            end)

order by 1 desc, lower(collateral_type) desc 