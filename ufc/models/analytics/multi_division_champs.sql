{{ config(materialized='view', alias='mv_multiple_weight_class_champs', schema='goat_status') }}

with title_wins as (
    select
        winner as fighter,
        trim(
            regexp_replace(
                regexp_replace(weightclass, '\\s+Title Bout\\s*$', '', 'i'),
                '^UFC\\s+',
                '',
                'i'
            )
        ) as weight_category,
        event_date
    from {{ ref('title_fights') }}
    where winner is not null
      and winner <> ''
      and weightclass is not null
      and weightclass not ilike '%interim%'
      and weightclass not ilike '%tournament%'
),
category_wins as (
    select
        fighter,
        weight_category,
        count(*) as title_wins_in_category,
        min(event_date) as first_title_win_date,
        max(event_date) as last_title_win_date
    from title_wins
    where weight_category is not null and weight_category <> ''
    group by fighter, weight_category
),
champ_counts as (
    select
        fighter,
        count(*) as distinct_title_categories,
        sum(title_wins_in_category) as total_title_wins
    from category_wins
    group by fighter
)
select
    champ_counts.fighter,
    champ_counts.distinct_title_categories,
    champ_counts.total_title_wins,
    case
        when champ_counts.distinct_title_categories >= 3 then 'triple'
        else 'double'
    end as champ_type
from champ_counts
where champ_counts.distinct_title_categories >= 2
order by champ_counts.distinct_title_categories desc,
    champ_counts.total_title_wins desc,
    champ_counts.fighter
