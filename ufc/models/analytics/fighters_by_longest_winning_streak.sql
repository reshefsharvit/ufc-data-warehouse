{{ config(materialized='view', alias='mv_fighters_by_longest_winning_streak', schema='goat_status') }}

with fights as (
    select
        results.url as fight_id,
        results.event,
        results.bout,
        events.event_date,
        results.fighter_1,
        results.fighter_2,
        results.outcome_1,
        results.outcome_2
    from {{ ref('stg_fight_results') }} as results
    left join {{ ref('stg_event_details') }} as events
      on results.event = events.event
    where results.fighter_1 is not null
      and results.fighter_2 is not null
),
fighter_fights as (
    select
        fight_id,
        event,
        bout,
        event_date,
        fighter_1 as fighter,
        upper(outcome_1) as outcome
    from fights
    union all
    select
        fight_id,
        event,
        bout,
        event_date,
        fighter_2 as fighter,
        upper(outcome_2) as outcome
    from fights
),
ordered as (
    select
        *,
        row_number() over (
            partition by fighter
            order by event_date, event, bout, fight_id
        ) as fight_seq,
        sum(case when outcome = 'W' then 0 else 1 end) over (
            partition by fighter
            order by event_date, event, bout, fight_id
            rows between unbounded preceding and current row
        ) as loss_group
    from fighter_fights
    where fighter is not null and fighter <> ''
),
streaks as (
    select
        fighter,
        loss_group,
        count(*) as streak_length
    from ordered
    where outcome = 'W'
    group by fighter, loss_group
),
max_streaks as (
    select
        fighter,
        max(streak_length) as longest_win_streak
    from streaks
    group by fighter
),
filtered as (
    select
        fighter,
        longest_win_streak
    from max_streaks
    where longest_win_streak >= 10
)
select
    filtered.fighter,
    filtered.longest_win_streak
from filtered
order by filtered.longest_win_streak desc, filtered.fighter
