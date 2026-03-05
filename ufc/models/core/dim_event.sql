{{ config(materialized='view', alias='dim_event', schema='semantic') }}

select
    event as event_name,
    event as event_key,
    event_date,
    event_year,
    location,
    url
from {{ ref('stg_event_details') }}
where event is not null and event <> ''
