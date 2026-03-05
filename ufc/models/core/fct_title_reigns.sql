{{ config(materialized='view', alias='fct_title_reigns', schema='semantic') }}

select
    weight_category,
    fighter,
    start_date,
    end_date,
    end_reason,
    reign_days,
    is_active
from {{ ref('title_reigns') }}
