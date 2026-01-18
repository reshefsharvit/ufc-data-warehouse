{{ config(materialized='view', alias='mv_quality_of_opposition_faced', schema='goat_status') }}

with fights as (
    select
        results.url as fight_id,
        results.event,
        results.bout,
        events.event_date,
        results.fighter_1,
        results.fighter_2,
        results.winner
    from {{ ref('stg_fight_results') }} as results
    join {{ ref('stg_event_details') }} as events
      on results.event = events.event
    where results.winner is not null
      and results.winner <> ''
),
fighter_rows as (
    select
        fight_id,
        event,
        bout,
        event_date,
        fighter_1 as fighter,
        fighter_2 as opponent,
        case when winner = fighter_1 then 1 else 0 end as is_win
    from fights
    union all
    select
        fight_id,
        event,
        bout,
        event_date,
        fighter_2 as fighter,
        fighter_1 as opponent,
        case when winner = fighter_2 then 1 else 0 end as is_win
    from fights
),
fighter_records as (
    select
        fight_id,
        event,
        bout,
        event_date,
        fighter,
        opponent,
        is_win,
        case when is_win = 1 then 0 else 1 end as is_loss,
        sum(is_win) over (
            partition by fighter
            order by event_date, event, bout, fight_id
            rows between unbounded preceding and 1 preceding
        ) as prior_wins,
        sum(case when is_win = 1 then 0 else 1 end) over (
            partition by fighter
            order by event_date, event, bout, fight_id
            rows between unbounded preceding and 1 preceding
        ) as prior_losses,
        sum(is_win) over (partition by fighter) as career_wins
    from fighter_rows
)
select
    fighter_records.fighter,
    count(*) as fights,
    max(fighter_records.career_wins) as wins,
    avg(
        case
            when opponent_records.prior_wins + opponent_records.prior_losses > 0
                then opponent_records.prior_wins::float
                    / (opponent_records.prior_wins + opponent_records.prior_losses)
            else null
        end
    ) as avg_opponent_win_pct_at_time,
    avg(opponent_records.career_wins::float) as avg_opponent_career_wins
from fighter_records
join fighter_records as opponent_records
  on fighter_records.fight_id = opponent_records.fight_id
  and fighter_records.opponent = opponent_records.fighter
where fighter_records.fighter is not null
  and fighter_records.fighter <> ''
group by fighter_records.fighter
having max(fighter_records.career_wins) >= 10
order by wins desc,
    avg_opponent_win_pct_at_time desc,
    avg_opponent_career_wins desc,
    fighter_records.fighter
