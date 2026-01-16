{{ config(materialized='view', alias='mv_consecutive_title_defenses', schema='goat_status') }}

with title_fights as (
    select
        results.weightclass,
        results.fighter_1,
        results.fighter_2,
        results.outcome_1,
        results.outcome_2,
        events.event_date
    from {{ ref('stg_fight_results') }} as results
    join {{ ref('stg_event_details') }} as events
      on results.event = events.event
    where results.weightclass ilike '%Title Bout%'
      and results.weightclass not ilike '%interim%'
      and results.weightclass not ilike '%tournament%'
      and events.event_date is not null
),
normalized as (
    select
        event_date,
        trim(
            regexp_replace(
                regexp_replace(weightclass, '\\s+Title Bout\\s*$', '', 'i'),
                '^UFC\\s+',
                '',
                'i'
            )
        ) as weight_category,
        fighter_1,
        fighter_2,
        outcome_1,
        outcome_2
    from title_fights
),
fighter_results as (
    select
        fighter_1 as fighter,
        weight_category,
        event_date,
        case
            when upper(outcome_1) = 'W' then 'W'
            when upper(outcome_1) = 'L' then 'L'
            else null
        end as result
    from normalized
    union all
    select
        fighter_2 as fighter,
        weight_category,
        event_date,
        case
            when upper(outcome_2) = 'W' then 'W'
            when upper(outcome_2) = 'L' then 'L'
            else null
        end as result
    from normalized
),
ordered as (
    select
        fighter,
        weight_category,
        event_date,
        result,
        sum(case when result = 'L' then 1 else 0 end) over (
            partition by fighter, weight_category
            order by event_date
            rows between unbounded preceding and current row
        ) as loss_group
    from fighter_results
    where fighter is not null
      and fighter <> ''
      and weight_category is not null
      and weight_category <> ''
      and result in ('W', 'L')
),
streaks as (
    select
        fighter,
        weight_category,
        loss_group,
        count(*) filter (where result = 'W') as win_streak
    from ordered
    group by fighter, weight_category, loss_group
),
agg as (
    select
        fighter,
        weight_category,
        max(greatest(win_streak - 1, 0)) as max_consecutive_title_defenses,
        max(win_streak) as max_consecutive_title_wins
    from streaks
    group by fighter, weight_category
    having max(greatest(win_streak - 1, 0)) >= 1
)
select
    fighter,
    weight_category,
    max_consecutive_title_defenses,
    max_consecutive_title_wins
from agg
order by max_consecutive_title_defenses desc, max_consecutive_title_wins desc, fighter, weight_category
