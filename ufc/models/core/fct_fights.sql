{{ config(materialized='view', alias='fct_fights', schema='semantic') }}

with base as (
    select
        results.url as fight_id,
        results.event as event_name,
        events.event_date,
        results.bout,
        results.weightclass as weightclass_raw,
        trim(
            regexp_replace(
                regexp_replace(
                    regexp_replace(results.weightclass, '[[:space:]]+Title Bout[[:space:]]*$', '', 'i'),
                    '^UFC[[:space:]]+',
                    '',
                    'i'
                ),
                '^Interim[[:space:]]+',
                '',
                'i'
            )
        ) as weight_category,
        results.fighter_1,
        results.fighter_2,
        results.winner,
        results.method,
        results.round_number,
        results.time,
        results.time_format,
        results.method_group,
        results.fight_time_seconds,
        results.fight_time_minutes,
        case
            when results.weightclass ilike '%Title Bout%' then 1 else 0
        end as is_title_bout,
        case
            when results.weightclass ilike '%interim%' then 1 else 0
        end as is_interim,
        case
            when results.weightclass ilike '%tournament%' then 1 else 0
        end as is_tournament
    from {{ ref('stg_fight_results') }} as results
    left join {{ ref('stg_event_details') }} as events
      on results.event = events.event
)
select
    fight_id,
    event_name,
    event_date,
    bout,
    weightclass_raw,
    weight_category,
    fighter_1,
    fighter_2,
    winner,
    method,
    round_number,
    time,
    time_format,
    method_group,
    fight_time_seconds,
    fight_time_minutes,
    is_title_bout,
    is_interim,
    is_tournament
from base
where fight_id is not null and fight_id <> ''
