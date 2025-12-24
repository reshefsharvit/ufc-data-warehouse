{{ config(materialized='view', alias='mv_clutch_wins_min_10_fights', schema='goat_status') }}

with fighter_fights as (
    select fighter_1 as fighter
    from {{ ref('stg_fight_results') }}
    union all
    select fighter_2 as fighter
    from {{ ref('stg_fight_results') }}
),
fighter_totals as (
    select
        fighter,
        count(*) as total_fights
    from fighter_fights
    where fighter is not null and fighter <> ''
    group by fighter
),
agg as (
    select
        results.winner as fighter,
        count(*) as clutch_wins
    from {{ ref('stg_fight_results') }} as results
    join fighter_totals
      on results.winner = fighter_totals.fighter
    where results.round_number >= 4
      and results.method_group in ('KO/TKO', 'Submission')
      and results.winner is not null
      and results.winner <> ''
      and fighter_totals.total_fights >= 10
    group by results.winner
)
select
    agg.fighter,
    agg.clutch_wins
from agg
order by agg.clutch_wins desc, agg.fighter
