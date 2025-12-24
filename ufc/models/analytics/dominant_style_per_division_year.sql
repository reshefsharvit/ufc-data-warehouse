{{ config(materialized='view', alias='mv_dominant_style_per_division_year') }}

with fights as (
    select
        results.weightclass,
        results.method_group,
        events.event_year
    from {{ ref('stg_fight_results') }} as results
    join {{ ref('stg_event_details') }} as events
      on results.event = events.event
    where results.weightclass is not null
      and results.method_group is not null
      and events.event_year is not null
),
counts as (
    select
        weightclass,
        event_year,
        method_group,
        count(*) as fight_count
    from fights
    group by weightclass, event_year, method_group
),
ranked as (
    select
        weightclass,
        event_year,
        method_group,
        fight_count,
        dense_rank() over (
            partition by weightclass, event_year
            order by fight_count desc, method_group
        ) as style_rank
    from counts
)
select
    weightclass,
    event_year,
    method_group as winning_method,
    fight_count
from ranked
where style_rank = 1
order by fight_count desc, weightclass, event_year, winning_method
