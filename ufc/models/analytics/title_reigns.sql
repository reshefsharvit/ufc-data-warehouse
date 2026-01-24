{{ config(materialized='view', alias='mv_title_reigns', schema='goat_status') }}

with title_fights as (
    select
        results.event,
        events.event_date,
        results.weightclass,
        results.fighter_1,
        results.fighter_2,
        results.winner
    from {{ ref('stg_fight_results') }} as results
    join {{ ref('stg_event_details') }} as events
      on results.event = events.event
    where results.weightclass ilike '%Title Bout%'
      and results.weightclass not ilike '%interim%'
      and results.weightclass not ilike '%tournament%'
      and results.winner is not null
),
normalized_title_fights as (
    select
        event,
        event_date,
        winner,
        fighter_1,
        fighter_2,
        trim(
            regexp_replace(
                regexp_replace(weightclass, '[[:space:]]+Title Bout[[:space:]]*$', '', 'i'),
                '^UFC[[:space:]]+',
                '',
                'i'
            )
        ) as weight_category
    from title_fights
    where weightclass is not null and weightclass <> ''
),
interim_title_fights as (
    select
        results.event,
        events.event_date,
        results.weightclass,
        results.fighter_1,
        results.fighter_2,
        results.winner
    from {{ ref('stg_fight_results') }} as results
    join {{ ref('stg_event_details') }} as events
      on results.event = events.event
    where results.weightclass ilike '%Title Bout%'
      and results.weightclass ilike '%interim%'
      and results.weightclass not ilike '%tournament%'
      and results.winner is not null
),
normalized_interim_fights as (
    select
        event,
        event_date,
        winner,
        fighter_1,
        fighter_2,
        trim(
            regexp_replace(
                regexp_replace(
                    regexp_replace(weightclass, '[[:space:]]+Title Bout[[:space:]]*$', '', 'i'),
                    '^UFC[[:space:]]+',
                    '',
                    'i'
                ),
                '^Interim[[:space:]]+',
                '',
                'i'
            )
        ) as weight_category
    from interim_title_fights
    where weightclass is not null and weightclass <> ''
),
champion_events as (
    select
        weight_category,
        winner as fighter,
        event_date as start_date,
        event,
        lag(winner) over (
            partition by weight_category
            order by event_date, event
        ) as previous_champion
    from normalized_title_fights
),
latest_undisputed as (
    select
        weight_category,
        max(event_date) as last_undisputed_date
    from normalized_title_fights
    group by weight_category
),
latest_interim as (
    select
        weight_category,
        winner as fighter,
        event_date as start_date,
        event,
        row_number() over (
            partition by weight_category
            order by event_date desc, event desc
        ) as interim_rank
    from normalized_interim_fights
),
interim_fallback as (
    select
        interim.weight_category,
        interim.fighter,
        interim.start_date,
        null::date as next_champion_date
    from latest_interim as interim
    left join latest_undisputed as undisputed
      on interim.weight_category = undisputed.weight_category
    where interim.interim_rank = 1
      and (
          undisputed.last_undisputed_date is null
          or undisputed.last_undisputed_date < interim.start_date
      )
),
new_champion_events as (
    select
        weight_category,
        fighter,
        start_date,
        lead(start_date) over (
            partition by weight_category
            order by start_date, event
        ) as next_champion_date
    from champion_events
    where previous_champion is null or fighter <> previous_champion
    union all
    select
        weight_category,
        fighter,
        start_date,
        next_champion_date
    from interim_fallback
),
fighter_name_lookup as (
    select distinct
        weight_category,
        fighter_name,
        split_part(fighter_name, ' ', 1) as first_name,
        split_part(
            fighter_name,
            ' ',
            array_length(string_to_array(fighter_name, ' '), 1)
        ) as last_name
    from (
        select weight_category, fighter_1 as fighter_name
        from normalized_title_fights
        union all
        select weight_category, fighter_2 as fighter_name
        from normalized_title_fights
    ) as fighters
    where fighter_name is not null and fighter_name <> ''
),
status_changes as (
    select
        row_number() over (order by change_date, fighter, weight_category) as status_id,
        change_date,
        trim(
            regexp_replace(
                regexp_replace(weight_category, '[[:space:]]+Championship[[:space:]]*$', '', 'i'),
                '^UFC[[:space:]]+',
                '',
                'i'
            )
        ) as weight_category,
        trim(
            regexp_replace(
                regexp_replace(
                    regexp_replace(fighter, '^Both\\s+', '', 'i'),
                    '[[:space:]]*\\(.*\\)[[:space:]]*',
                    '',
                    'i'
                ),
                '[[:space:]]+retired[[:space:]]*$',
                '',
                'i'
            )
        ) as fighter_token,
        reason
    from (
        select
            case
                when date ~ '^[A-Za-z]{3}[[:space:]]+[0-9]{1,2},[[:space:]]+[0-9]{4}$' then
                    to_date(date, 'Mon DD, YYYY')
                when date ~ '^[A-Za-z]+[[:space:]]+[0-9]{1,2},[[:space:]]+[0-9]{4}$' then
                    to_date(date, 'Month DD, YYYY')
                when date ~ '^[A-Za-z]{3}[[:space:]]+[0-9]{4}$' then
                    to_date(date, 'Mon YYYY')
                when date ~ '^[A-Za-z]+[[:space:]]+[0-9]{4}$' then
                    to_date(date, 'Month YYYY')
                else null
            end as change_date,
            fighter,
            weight_category,
            reason
        from {{ source('ufc', 'title_status_changes_outside_octagon') }}
    ) as raw
    where change_date is not null
      and fighter is not null and fighter <> ''
      and weight_category is not null and weight_category <> ''
),
status_fighter_matches as (
    select
        status.status_id,
        status.change_date,
        status.weight_category,
        status.fighter_token,
        status.reason,
        lookup.fighter_name,
        case
            when lower(status.fighter_token) = lower(lookup.fighter_name) then 3
            when lower(status.fighter_token) = lower(lookup.last_name) then 2
            when lower(status.fighter_token) = lower(lookup.first_name) then 2
            when lower(lookup.fighter_name) like '%' || lower(status.fighter_token) || '%' then 1
            else 0
        end as match_score
    from status_changes as status
    join fighter_name_lookup as lookup
      on status.weight_category = lookup.weight_category
     and (
         lower(status.fighter_token) = lower(lookup.fighter_name)
         or lower(status.fighter_token) = lower(lookup.first_name)
         or lower(status.fighter_token) = lower(lookup.last_name)
         or lower(lookup.fighter_name) like '%' || lower(status.fighter_token) || '%'
     )
),
status_best_match as (
    select
        status_id,
        fighter_name
    from (
        select
            status_id,
            fighter_name,
            row_number() over (
                partition by status_id
                order by match_score desc, fighter_name
            ) as match_rank
        from status_fighter_matches
    ) as ranked
    where match_rank = 1
),
status_resolved as (
    select
        status.status_id,
        status.change_date,
        status.weight_category,
        coalesce(match.fighter_name, status.fighter_token) as fighter_full_name,
        status.reason
    from status_changes as status
    left join status_best_match as match
      on status.status_id = match.status_id
),
reigns_with_status as (
    select
        reigns.weight_category,
        reigns.fighter,
        reigns.start_date,
        reigns.next_champion_date,
        status.change_date as status_end_date,
        status.reason as status_end_reason,
        row_number() over (
            partition by reigns.weight_category, reigns.fighter, reigns.start_date
            order by status.change_date
        ) as status_rank
    from new_champion_events as reigns
    left join status_resolved as status
      on status.weight_category = reigns.weight_category
     and (
         lower(status.fighter_full_name) = lower(reigns.fighter)
         or lower(status.fighter_full_name) = lower(
             split_part(
                 reigns.fighter,
                 ' ',
                 array_length(string_to_array(reigns.fighter, ' '), 1)
             )
         )
     )
     and status.change_date >= reigns.start_date
     and (reigns.next_champion_date is null or status.change_date < reigns.next_champion_date)
),
reigns_final as (
    select
        weight_category,
        fighter,
        start_date,
        coalesce(status_end_date, next_champion_date) as end_date,
        case
            when status_end_date is not null then status_end_reason
            when next_champion_date is not null then 'lost title'
            else null
        end as end_reason
    from reigns_with_status
    where status_rank = 1 or status_rank is null
)
select
    weight_category,
    fighter,
    start_date::date as start_date,
    end_date::date as end_date,
    end_reason,
    case
        when end_date is null then null
        else (end_date - start_date)
    end as reign_days,
    case
        when end_date is null then 1
        else 0
    end as is_active
from reigns_final
order by reign_days desc nulls last, weight_category, fighter
