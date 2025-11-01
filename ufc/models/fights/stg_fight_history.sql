{{ config(materialized='view', schema='fights') }}

with base as (
    select fighter_id, fight_history
    from {{ source('fighters_data','fighter_fight_history') }}
),

exploded as (
    select
        b.fighter_id,
        kv.key   as fight_key,
        kv.value as fight_json
    from base b,
         lateral jsonb_each(b.fight_history) as kv(key, value)
),

typed as (
    select
        e.fighter_id,
        e.fight_key,
        e.fight_json->>'event_id'  as event_id,
        e.fight_json->>'event'     as event_name,

        /* Robust date: drop dots in month names; if it doesn't match, returns NULL */
        to_date(
          regexp_replace(e.fight_json->>'date', '\.', '', 'g'),
          'Mon DD, YYYY'
        )                          as fight_date,

        e.fight_json->>'fighter1_id' as fighter1_id,
        e.fight_json->>'fighter1'    as fighter1_name,
        e.fight_json->>'fighter2_id' as fighter2_id,
        e.fight_json->>'fighter2'    as fighter2_name,

        e.fight_json->>'winner_id'   as winner_id,
        e.fight_json->>'winner'      as winner_name,
        e.fight_json->>'loser_id'    as loser_id,
        e.fight_json->>'loser'       as loser_name,

        e.fight_json->>'method'      as method,

        /* Only cast if strictly digits; else NULL */
        case
          when (e.fight_json->>'round') ~ '^\d+$'
            then (e.fight_json->>'round')::int
        end                        as round_num,

        /* keep raw time string; we'll parse safely downstream */
        e.fight_json->>'time'      as time_str
    from exploded e
)

select * from typed
