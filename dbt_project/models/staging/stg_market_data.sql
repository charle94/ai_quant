{{ config(materialized='view') }}

with source_data as (
    select
        date,
        symbol,
        market_cap,
        pe_ratio,
        dividend_yield,
        sector,
        -- 添加计算字段
        case 
            when pe_ratio > 0 then market_cap / pe_ratio 
            else null 
        end as estimated_earnings,
        case 
            when dividend_yield > 0 then market_cap * dividend_yield / 100 
            else 0 
        end as estimated_dividend_payout
    from {{ ref('market_data') }}
    where date is not null
      and symbol is not null
      and market_cap > 0
)

select * from source_data