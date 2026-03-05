{{ config(materialized='view', alias='dim_division', schema='semantic') }}

select distinct
    weight_category as division_name
from {{ ref('fct_fights') }}
where weight_category is not null and weight_category <> ''
