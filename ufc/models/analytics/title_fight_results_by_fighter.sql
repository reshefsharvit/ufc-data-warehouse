{{ config(materialized='view', alias='mv_title_fight_results_by_fighter', schema='goat_status') }}

with title_fights as (
    select
        weightclass,
        fighter_1,
        fighter_2,
        outcome_1,
        outcome_2
    from {{ ref('stg_fight_results') }}
    where weightclass ilike '%Title Bout%'
),
fighter_results as (
    select
        fighter_1 as fighter,
        weightclass as category,
        case when upper(outcome_1) = 'W' then 1 else 0 end as title_fight_win,
        case when upper(outcome_1) = 'L' then 1 else 0 end as title_fight_loss
    from title_fights
    union all
    select
        fighter_2 as fighter,
        weightclass as category,
        case when upper(outcome_2) = 'W' then 1 else 0 end as title_fight_win,
        case when upper(outcome_2) = 'L' then 1 else 0 end as title_fight_loss
    from title_fights
),
agg as (
    select
        fighter,
        category,
        sum(title_fight_win) as title_fight_wins,
        sum(title_fight_loss) as title_fight_losses
    from fighter_results
    where fighter is not null and fighter <> ''
    group by fighter, category
)
select
    agg.fighter,
    agg.category,
    agg.title_fight_wins,
    agg.title_fight_losses
from agg
order by agg.title_fight_wins desc, agg.title_fight_losses asc, agg.fighter
