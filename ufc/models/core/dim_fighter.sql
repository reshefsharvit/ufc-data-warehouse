{{ config(materialized='view', alias='dim_fighter', schema='semantic') }}

with fighters as (
    select fighter_1 as fighter_name
    from {{ ref('fct_fights') }}
    union all
    select fighter_2 as fighter_name
    from {{ ref('fct_fights') }}
    union all
    select winner as fighter_name
    from {{ ref('fct_fights') }}
)
select distinct
    fighter_name,
    split_part(fighter_name, ' ', 1) as first_name,
    split_part(
        fighter_name,
        ' ',
        array_length(string_to_array(fighter_name, ' '), 1)
    ) as last_name
from fighters
where fighter_name is not null and fighter_name <> ''
