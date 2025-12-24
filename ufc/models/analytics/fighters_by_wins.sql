{{ config(materialized='view', alias='mv_fighters_by_wins') }}

select
    winner as fighter,
    count(*) as wins
from {{ ref('stg_fight_results') }}
where winner is not null
  and winner <> ''
group by winner
order by wins desc
