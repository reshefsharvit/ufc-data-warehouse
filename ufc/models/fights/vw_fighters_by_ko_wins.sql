{{ config(materialized='view', schema='fights') }}

with ko_wins as (
  select
    winner_id as fighter_id,
    count(*)  as wins_ko
  from {{ ref('fact_fights') }}
  where winner_id is not null
    and method ilike '%KO%'
  group by 1
),
with_names as (
  select
    k.fighter_id,
    f.document->>'name'      as name,
    f.document->>'division'  as division,
    k.wins_ko
  from ko_wins k
  left join {{ source('fighters_data','fighters') }} f
    on f.fighter_id = k.fighter_id
)

select *
from with_names
order by wins_ko desc, name
