{{ config(materialized='table', schema='fights') }}

with f as (
  select
    fighter_id,
    document->>'name' as name,
    document
  from {{ source('fighters_data','fighters') }}
)

select distinct fighter_id, name, document as about_json
from f
