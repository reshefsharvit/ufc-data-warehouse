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
    case
        when event_date_raw ~ '^[A-Za-z]{3}[[:space:]]+[0-9]{1,2},[[:space:]]+[0-9]{4}$' then
            to_date(event_date_raw, 'Mon DD, YYYY')
        when event_date_raw ~ '^[A-Za-z]+[[:space:]]+[0-9]{1,2},[[:space:]]+[0-9]{4}$' then
            to_date(event_date_raw, 'Month DD, YYYY')
        when event_date_raw ~ '^[A-Za-z]{3}[[:space:]]+[0-9]{4}$' then
            to_date(event_date_raw, 'Mon YYYY')
        when event_date_raw ~ '^[A-Za-z]+[[:space:]]+[0-9]{4}$' then
            to_date(event_date_raw, 'Month YYYY')
        else null
    end as event_date,
    extract(
        year from case
            when event_date_raw ~ '^[A-Za-z]{3}[[:space:]]+[0-9]{1,2},[[:space:]]+[0-9]{4}$' then
                to_date(event_date_raw, 'Mon DD, YYYY')
            when event_date_raw ~ '^[A-Za-z]+[[:space:]]+[0-9]{1,2},[[:space:]]+[0-9]{4}$' then
                to_date(event_date_raw, 'Month DD, YYYY')
            when event_date_raw ~ '^[A-Za-z]{3}[[:space:]]+[0-9]{4}$' then
                to_date(event_date_raw, 'Mon YYYY')
            when event_date_raw ~ '^[A-Za-z]+[[:space:]]+[0-9]{4}$' then
                to_date(event_date_raw, 'Month YYYY')
            else null
        end
    )::int as event_year
from source
