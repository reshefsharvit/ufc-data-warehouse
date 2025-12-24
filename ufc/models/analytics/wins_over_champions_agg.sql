{{ config(materialized='view', alias='mv_wins_over_champions_agg', schema='goat_status') }}

select
    fighter,
    count(distinct opponent) as wins_over_champions
from {{ ref('wins_over_champions') }}
where fighter is not null
  and fighter <> ''
group by fighter
order by wins_over_champions desc, fighter
