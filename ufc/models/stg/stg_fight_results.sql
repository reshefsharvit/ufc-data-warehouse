{{ config(materialized='view', alias='mv_stg_fight_results') }}

with source as (
    select
        trim(event) as event,
        trim(bout) as bout,
        trim(weightclass) as weightclass,
        trim(method) as method,
        nullif(trim(round), '')::int as round_number,
        trim(time) as time,
        trim(time_format) as time_format,
        trim(url) as url,
        split_part(trim(bout), ' vs. ', 1) as fighter_1,
        split_part(trim(bout), ' vs. ', 2) as fighter_2,
        split_part(trim(outcome), '/', 1) as outcome_1,
        split_part(trim(outcome), '/', 2) as outcome_2
    from {{ source('ufc', 'fact_ufc_fight_results') }}
)

select
    event,
    bout,
    weightclass,
    method,
    round_number,
    time,
    time_format,
    url,
    fighter_1,
    fighter_2,
    outcome_1,
    outcome_2,
    case
        when upper(outcome_1) = 'W' then fighter_1
        when upper(outcome_2) = 'W' then fighter_2
        else null
    end as winner,
    case
        when method ilike '%KO%' then 'KO/TKO'
        when method ilike '%SUB%' then 'Submission'
        when method ilike '%DEC%' then 'Decision'
        else 'Other'
    end as method_group,
    case
        when time is null or position(':' in time) = 0 or round_number is null then null
        else ((round_number - 1) * 300)
            + (split_part(time, ':', 1)::int * 60)
            + (split_part(time, ':', 2)::int)
    end as fight_time_seconds,
    case
        when time is null or position(':' in time) = 0 or round_number is null then null
        else (((round_number - 1) * 300)
            + (split_part(time, ':', 1)::int * 60)
            + (split_part(time, ':', 2)::int)) / 60.0
    end as fight_time_minutes
from source
