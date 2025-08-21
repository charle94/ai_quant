
    
    

with all_values as (

    select
        volatility_regime as value_field,
        count(*) as n_records

    from "quant_features"."main"."alpha_factors_final"
    group by volatility_regime

)

select *
from all_values
where value_field not in (
    'HIGH_VOL','LOW_VOL','NORMAL_VOL'
)


