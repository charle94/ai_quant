
    
    

select
    entity_id as unique_field,
    count(*) as n_records

from "quant_features"."main"."alpha_factors_final"
where entity_id is not null
group by entity_id
having count(*) > 1


