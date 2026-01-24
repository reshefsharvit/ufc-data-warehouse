{{ config(materialized='view', alias='mv_total_champ_days', schema='goat_status') }}

with base as (
    select
        weight_category,
        fighter,
        start_date,
        coalesce(end_date, current_date) as effective_end_date
    from {{ ref('title_reigns') }}
    where start_date is not null
),
agg as (
    select
        weight_category,
        fighter,
        sum(effective_end_date - start_date) as total_champ_days
    from base
    group by weight_category, fighter
)
select
    weight_category,
    fighter,
    total_champ_days
from agg
order by total_champ_days desc, weight_category, fighter
