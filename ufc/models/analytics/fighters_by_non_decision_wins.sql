{{ config(materialized='view', alias='mv_fighters_by_non_decision_wins', schema='goat_status') }}

with agg as (
    select
        winner as fighter,
        count(*) as non_decision_wins
    from {{ ref('stg_fight_results') }}
    where method_group in ('KO/TKO', 'Submission')
      and winner is not null
      and winner <> ''
    group by winner
)
select
    agg.fighter,
    agg.non_decision_wins
from agg
order by agg.non_decision_wins desc, agg.fighter
