
    
    

with all_values as (

    select
        market_regime as value_field,
        count(*) as n_records

    from "quant_features"."main"."alpha_factors_final"
    group by market_regime

)

select *
from all_values
where value_field not in (
    'TRENDING','MEAN_REVERTING','SIDEWAYS'
)


