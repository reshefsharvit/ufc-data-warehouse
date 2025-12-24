{{ config(materialized='view', alias='mv_fighters_best_record_min_10_fights', schema='goat_status') }}

with fighter_results as (
    select
        fighter_1 as fighter,
        upper(outcome_1) as outcome
    from {{ ref('stg_fight_results') }}
    union all
    select
        fighter_2 as fighter,
        upper(outcome_2) as outcome
    from {{ ref('stg_fight_results') }}
),
counts as (
    select
        fighter,
        count(*) as total_fights,
        sum(case when outcome = 'W' then 1 else 0 end) as wins,
        sum(case when outcome = 'L' then 1 else 0 end) as losses,
        sum(case when outcome not in ('W', 'L') then 1 else 0 end) as nc
    from fighter_results
    where fighter is not null and fighter <> ''
    group by fighter
),
filtered as (
    select
        fighter,
        wins,
        losses,
        nc,
        total_fights,
        case when total_fights > 0 then wins::float / total_fights else null end as win_pct
    from counts
    where total_fights >= 10
)
select
    filtered.fighter,
    filtered.wins,
    filtered.losses,
    filtered.nc,
    filtered.total_fights,
    filtered.win_pct
from filtered
order by filtered.win_pct desc, filtered.wins desc, filtered.losses asc, filtered.fighter
