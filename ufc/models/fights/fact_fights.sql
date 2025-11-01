{{ config(materialized='table', schema='fights') }}

with stg as (
    select * from {{ ref('stg_fight_history') }}
),

-- normalize time to seconds safely
time_parsed as (
    select
        stg.*,
        case
            when stg.time_str ~ '^\d{1,2}:\d{2}$'
                then split_part(stg.time_str, ':', 1)::int * 60
                   + split_part(stg.time_str, ':', 2)::int
            else null
        end as time_seconds
    from stg
),

-- make fighter pair order-independent to avoid A-vs-B vs B-vs-A differences
pair_norm as (
    select
        t.*,
        least(coalesce(t.fighter1_id,''), coalesce(t.fighter2_id,'')) as pair_left,
        greatest(coalesce(t.fighter1_id,''), coalesce(t.fighter2_id,'')) as pair_right
    from time_parsed t
),

-- stronger bout_id: include event_id, date, sorted pair, method, round, time
keyed as (
    select
        md5(
            coalesce(event_id,'') || '|' ||
            coalesce(fight_date::text,'') || '|' ||
            coalesce(pair_left,'') || '|' ||
            coalesce(pair_right,'') || '|' ||
            coalesce(method,'') || '|' ||
            coalesce(round_num::text,'') || '|' ||
            coalesce(time_str,'')
        ) as bout_id,
        *
    from pair_norm
),

-- if duplicates still exist after the stronger key, pick one deterministically
dedup as (
    select
        *,
        row_number() over (
            partition by bout_id
            order by
                -- prefer rows that have more complete info
                (winner_id is not null)::int desc,
                (loser_id  is not null)::int desc,
                (method    is not null)::int desc,
                (time_seconds is not null)::int desc,
                fight_key  -- stable tie-breaker from source JSON
        ) as _rn
    from keyed
)

select
    bout_id,
    event_id,
    event_name,
    fight_date,

    fighter1_id,
    fighter1_name,
    fighter2_id,
    fighter2_name,

    winner_id,
    winner_name,
    loser_id,
    loser_name,

    case
        when winner_id = fighter1_id then 1
        when winner_id = fighter2_id then 2
        else 0
        end as winner_flag,

    method,
    round_num,
    time_str,
    time_seconds
from dedup
where _rn = 1
