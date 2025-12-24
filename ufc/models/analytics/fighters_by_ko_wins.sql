{{ config(materialized='view', alias='mv_fighters_by_ko_wins') }}

select
    winner as fighter,
    count(*) as ko_wins
from {{ ref('stg_fight_results') }}
where method_group = 'KO/TKO'
  and winner is not null
  and winner <> ''
group by winner
order by ko_wins desc
