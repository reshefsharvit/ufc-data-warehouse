{{ config(materialized='view', schema='fights') }}

with wins as (
  select winner_id as fighter_id, count(*) as wins
  from {{ ref('fact_fights') }}
  where winner_id is not null
  group by 1
),
losses as (
  select loser_id as fighter_id, count(*) as losses
  from {{ ref('fact_fights') }}
  where loser_id is not null
  group by 1
),
combined as (
  select
    coalesce(w.fighter_id, l.fighter_id) as fighter_id,
    coalesce(w.wins, 0)  as wins,
    coalesce(l.losses, 0) as losses
  from wins w
  full outer join losses l on w.fighter_id = l.fighter_id
),
with_names as (
  select
    c.fighter_id,
    f.document->>'name'      as name,
    f.document->>'division'  as division,
    c.wins,
    c.losses,
    (c.wins + c.losses)      as total_bouts,
    round(
      (c.wins::numeric / nullif(c.wins + c.losses, 0)::numeric),
      3
    ) as win_rate
  from combined c
  left join {{ source('fighters_data','fighters') }} f
    on f.fighter_id = c.fighter_id
)

select *
from with_names
order by wins desc, name
