{{ config(materialized='view', alias='mv_wins_over_champions') }}

with title_wins as (
    select
        results.winner as fighter,
        events.event_date
    from {{ ref('stg_fight_results') }} as results
    join {{ ref('stg_event_details') }} as events
      on results.event = events.event
    where results.winner is not null
      and results.winner <> ''
      and results.weightclass ilike '%Title Bout%'
      and results.weightclass not ilike '%interim%'
      and results.weightclass not ilike '%tournament%'
),
champions as (
    select
        fighter,
        min(event_date) as first_title_win_date
    from title_wins
    group by fighter
),
fights as (
    select
        results.url as fight_id,
        results.event,
        results.bout,
        results.weightclass,
        results.winner,
        results.fighter_1,
        results.fighter_2,
        events.event_date
    from {{ ref('stg_fight_results') }} as results
    join {{ ref('stg_event_details') }} as events
      on results.event = events.event
    where results.winner is not null
      and results.winner <> ''
),
with_opponent as (
    select
        fight_id,
        event,
        bout,
        weightclass,
        event_date,
        winner as fighter,
        case when winner = fighter_1 then fighter_2 else fighter_1 end as opponent
    from fights
)
select
    with_opponent.fighter,
    with_opponent.opponent,
    with_opponent.event_date,
    with_opponent.event,
    with_opponent.bout,
    with_opponent.weightclass
from with_opponent
join champions
  on champions.fighter = with_opponent.opponent
where champions.first_title_win_date <= with_opponent.event_date
order by with_opponent.event_date desc,
    with_opponent.fighter,
    with_opponent.opponent
