{{ config(materialized='view', alias='mv_title_fights') }}

select
    results.event,
    results.bout,
    results.weightclass,
    results.method,
    results.round_number,
    results.time,
    results.time_format,
    results.winner,
    results.url,
    events.event_date,
    events.event_year,
    events.location
from {{ ref('stg_fight_results') }} as results
left join {{ ref('stg_event_details') }} as events
  on results.event = events.event
where results.weightclass ilike '%Title Bout%'
order by events.event_date desc nulls last, results.event, results.bout
