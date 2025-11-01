{{ config(materialized='view', schema='fights') }}

with f as (
  select
    ff.bout_id,
    ff.time_seconds,
    f1.document->>'division' as div1,
    f2.document->>'division' as div2
  from {{ ref('fact_fights') }} ff
  left join {{ source('fighters_data','fighters') }} f1
    on f1.fighter_id = ff.fighter1_id
  left join {{ source('fighters_data','fighters') }} f2
    on f2.fighter_id = ff.fighter2_id
  where ff.time_seconds is not null
),

-- choose the division for the bout
-- if both fighters have the same division, use it; otherwise fall back to whichever exists
divided as (
  select
    case
      when div1 is not null and div2 is not null and div1 = div2 then div1
      else coalesce(div1, div2, 'Unknown')
    end as division,
    time_seconds
  from f
),

agg as (
  select
    division,
    count(*)                                   as bouts_count,
    avg(time_seconds)::numeric                 as avg_seconds,
    percentile_cont(0.5) within group (order by time_seconds)::numeric as median_seconds,
    percentile_cont(0.9) within group (order by time_seconds)::numeric as p90_seconds,
    min(time_seconds)                          as min_seconds,
    max(time_seconds)                          as max_seconds
  from divided
  group by 1
  -- uncomment to require a minimum sample size per division
  -- having count(*) >= 10
),

formatted as (
  select
    division,
    bouts_count,
    avg_seconds,
    median_seconds,
    p90_seconds,
    min_seconds,
    max_seconds,
    -- pretty MM:SS for average
    lpad(((round(avg_seconds, 0)::int) / 60)::text, 2, '0')
      || ':' ||
    lpad(((round(avg_seconds, 0)::int) % 60)::text, 2, '0') as avg_time_mm_ss
  from agg
)

select *
from formatted
order by avg_seconds desc, division
