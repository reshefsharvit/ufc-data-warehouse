{{ config(materialized='view', alias='mv_fight_time_per_division') }}

select
    weightclass,
    count(*) as fight_count,
    avg(fight_time_minutes) as avg_fight_time_minutes,
    avg(fight_time_seconds) as avg_fight_time_seconds
from {{ ref('stg_fight_results') }}
where weightclass is not null
  and fight_time_minutes is not null
group by weightclass
order by avg_fight_time_minutes desc, fight_count desc, weightclass
