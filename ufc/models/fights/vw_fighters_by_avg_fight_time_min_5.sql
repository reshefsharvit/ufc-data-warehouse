{{ config(materialized='view', schema='fights') }}

with participants as (
  -- one row per fighter per bout
  select f.bout_id, f.fighter1_id as fighter_id, f.time_seconds
  from {{ ref('fact_fights') }} f
  where f.time_seconds is not null

  union all

  select f.bout_id, f.fighter2_id as fighter_id, f.time_seconds
  from {{ ref('fact_fights') }} f
  where f.time_seconds is not null
),

agg as (
  select
    p.fighter_id,
    count(*)                            as bouts_count,
    avg(p.time_seconds)::numeric        as avg_seconds
  from participants p
  group by 1
  having count(*) >= 5    -- ✅ only fighters with at least 5 bouts
),

formatted as (
  select
    a.fighter_id,
    a.bouts_count,
    a.avg_seconds,
    -- pretty MM:SS format (rounded)
    lpad(((round(a.avg_seconds, 0)::int) / 60)::text, 2, '0')
      || ':' ||
    lpad(((round(a.avg_seconds, 0)::int) % 60)::text, 2, '0') as avg_time_mm_ss
  from agg a
)

select
    f.fighter_id,
    src.document->>'name'      as name,
    src.document->>'division'  as division,
    f.bouts_count,
    f.avg_seconds,
    f.avg_time_mm_ss
from formatted f
    left join {{ source('fighters_data','fighters') }} src
on src.fighter_id = f.fighter_id
order by avg_seconds desc, name
