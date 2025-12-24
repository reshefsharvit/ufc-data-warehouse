{{ config(materialized='view', alias='mv_title_defenses', schema='goat_status') }}

with title_wins as (
    select
        results.winner as fighter,
        results.weightclass,
        events.event_date
    from {{ ref('stg_fight_results') }} as results
    join {{ ref('stg_event_details') }} as events
      on results.event = events.event
    where results.winner is not null
      and results.winner <> ''
      and results.weightclass ilike '%Title Bout%'
      and results.weightclass not ilike '%interim%'
      and results.weightclass not ilike '%tournament%'
),
normalized as (
    select
        fighter,
        trim(
            regexp_replace(
                regexp_replace(weightclass, '\\s+Title Bout\\s*$', '', 'i'),
                '^UFC\\s+',
                '',
                'i'
            )
        ) as weight_category,
        event_date
    from title_wins
),
ordered as (
    select
        fighter,
        weight_category,
        event_date,
        row_number() over (
            partition by fighter, weight_category
            order by event_date
        ) as title_win_number
    from normalized
    where weight_category is not null and weight_category <> ''
),
defenses as (
    select
        fighter,
        weight_category,
        case when title_win_number > 1 then 1 else 0 end as is_defense
    from ordered
),
agg as (
    select
        fighter,
        weight_category,
        sum(is_defense) as title_defenses
    from defenses
    group by fighter, weight_category
    having sum(is_defense) > 0
)
select
    agg.fighter,
    agg.weight_category,
    agg.title_defenses
from agg
order by agg.weight_category, agg.title_defenses desc, agg.fighter
