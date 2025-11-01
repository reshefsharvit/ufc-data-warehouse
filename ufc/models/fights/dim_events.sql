{{ config(
    materialized = 'table',
    schema = 'fights'
) }}

-- DIM: fights.dim_events
-- Grain: one row per event_id
-- Source: stg_fight_history

select distinct
    sfh.event_id,
    sfh.event_name,
    sfh.fight_date
from {{ ref('stg_fight_history') }} as sfh
where sfh.event_id is not null
