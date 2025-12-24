{{ config(materialized='view', alias='mv_fightrers_by_avg_fight_time_min_5') }}

with base_fights as (
    select
        fighter_1,
        fighter_2,
        fight_time_minutes
    from {{ ref('stg_fight_results') }}
    where fight_time_minutes is not null
),
fighters as (
    select fighter_1 as fighter, fight_time_minutes from base_fights
    union all
    select fighter_2 as fighter, fight_time_minutes from base_fights
),
agg as (
    select
        fighter,
        count(*) as fight_count,
        avg(fight_time_minutes) as avg_fight_time_minutes
    from fighters
    where fighter is not null and fighter <> ''
    group by fighter
)
select
    fighter,
    fight_count,
    avg_fight_time_minutes
from agg
where avg_fight_time_minutes >= 5
order by avg_fight_time_minutes desc, fight_count desc
