{{ config(materialized='view', schema='fights') }}

with winners as (
  select
    ff.winner_id as fighter_id
  from {{ ref('fact_fights') }} ff
  where ff.winner_id is not null
),

winners_with_style as (
  select
    f.document->>'division' as division,
    lower(trim(f.document->>'Fighting style')) as raw_style
  from winners w
  left join {{ source('fighters_data','fighters') }} f
    on f.fighter_id = w.fighter_id
  where f.document->>'division' is not null
),

-- clean up and normalize styles
normalized as (
  select
    division,
    case
      when raw_style in ('mma', 'mixed martial arts', 'mixed martial artist') then null
      when raw_style in ('brazilian jiu-jitsu', 'bjj') then 'Brazilian Jiu-Jitsu'
      when raw_style like '%kickbox%' then 'Kickboxing'
      when raw_style like '%boxing%' then 'Boxing'
      when raw_style like '%wrestl%' then 'Wrestling'
      when raw_style like '%muay thai%' then 'Muay Thai'
      when raw_style like '%karate%' then 'Karate'
      when raw_style like '%taekwondo%' then 'Taekwondo'
      else initcap(raw_style)
    end as fighting_style
  from winners_with_style
  where raw_style is not null
),

counts as (
  select
    division,
    fighting_style,
    count(*)::int as wins_with_style
  from normalized
  where fighting_style is not null
  group by 1, 2
),

totals as (
  select
    division,
    sum(wins_with_style)::int as total_wins
  from counts
  group by 1
),

ranked as (
  select
    c.division,
    c.fighting_style,
    c.wins_with_style,
    t.total_wins,
    (c.wins_with_style::numeric / nullif(t.total_wins, 0)::numeric) as style_share,
    row_number() over (
      partition by c.division
      order by c.wins_with_style desc, c.fighting_style
    ) as rn
  from counts c
  join totals t
    on t.division = c.division
)

select
    division,
    fighting_style as dominant_style,
    wins_with_style,
    total_wins,
    round(style_share, 3) as style_share
from ranked
where rn = 1
order by division
