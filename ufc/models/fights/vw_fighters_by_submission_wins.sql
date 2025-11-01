{{ config(materialized='view', schema='fights') }}

with sub_wins as (
  select
    winner_id as fighter_id,
    count(*)  as wins_submission
  from {{ ref('fact_fights') }}
  where winner_id is not null
    and (
      method ilike '%Submission%'  -- "Submission - Rear Naked Choke", etc.
      or method ilike '%SUB%'      -- short forms
    )
  group by 1
),
with_names as (
  select
    s.fighter_id,
    f.document->>'name'      as name,
    f.document->>'division'  as division,
    s.wins_submission
  from sub_wins s
  left join {{ source('fighters_data','fighters') }} f
    on f.fighter_id = s.fighter_id
)

select *
from with_names
order by wins_submission desc, name
