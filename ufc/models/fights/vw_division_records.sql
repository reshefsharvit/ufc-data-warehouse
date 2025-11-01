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
records as (
  select
    coalesce(w.fighter_id, l.fighter_id) as fighter_id,
    coalesce(w.wins, 0)                  as wins,
    coalesce(l.losses, 0)                as losses,
    coalesce(w.wins, 0) + coalesce(l.losses, 0) as total_bouts
  from wins w
  full outer join losses l
    on w.fighter_id = l.fighter_id
),
with_division as (
  select
    r.fighter_id,
    f.document->>'name'      as name,
    nullif(f.document->>'division','') as division,
    r.wins,
    r.losses,
    r.total_bouts,
    case when r.total_bouts > 0
         then (r.wins::numeric / r.total_bouts::numeric)
    end as win_rate
  from records r
  left join {{ source('fighters_data','fighters') }} f
    on f.fighter_id = r.fighter_id
),
ranked as (
  select
    division,
    fighter_id,
    name,
    wins,
    losses,
    total_bouts,
    round(win_rate, 3) as win_rate,
    row_number() over (
      partition by division
      order by wins desc, win_rate desc, total_bouts desc, name
    ) as division_rank
  from with_division
)

select *
from ranked
order by division, division_rank, name   -- ✅ fixed: 'order by'
