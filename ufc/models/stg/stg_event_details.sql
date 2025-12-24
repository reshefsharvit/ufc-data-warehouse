{{ config(materialized='view', alias='mv_stg_event_details') }}

with source as (
    select
        trim(event) as event,
        trim(url) as url,
        trim(date) as event_date_raw,
        trim(location) as location
    from {{ source('ufc', 'dim_ufc_event_details') }}
)

select
    event,
    url,
    location,
    coalesce(
        to_date(event_date_raw, 'Month DD, YYYY'),
        to_date(event_date_raw, 'Mon DD, YYYY')
    ) as event_date,
    extract(
        year from coalesce(
            to_date(event_date_raw, 'Month DD, YYYY'),
            to_date(event_date_raw, 'Mon DD, YYYY')
        )
    )::int as event_year
from source
