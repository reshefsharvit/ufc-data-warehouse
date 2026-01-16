{{ config(materialized='view', alias='mv_championship_rounds_fought', schema='goat_status') }}

with title_fights as (
    select
        weightclass,
        fighter_1,
        fighter_2,
        round_number
    from {{ ref('stg_fight_results') }}
    where weightclass ilike '%Title Bout%'
      and weightclass not ilike '%interim%'
      and weightclass not ilike '%tournament%'
),
fighter_rounds as (
    select
        fighter_1 as fighter,
        round_number
    from title_fights
    union all
    select
        fighter_2 as fighter,
        round_number
    from title_fights
),
agg as (
    select
        fighter,
        count(*) as title_fights,
        sum(
            case
                when round_number = 4 then 1
                when round_number >= 5 then 2
                else 0
            end
        ) as championship_rounds_fought
    from fighter_rounds
    where fighter is not null
      and fighter <> ''
    group by fighter
    having count(*) >= 5
)
select
    fighter,
    title_fights,
    championship_rounds_fought
from agg
order by championship_rounds_fought desc, title_fights desc, fighter
