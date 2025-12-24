{{ config(materialized='view', alias='mv_fighters_by_submission_wins') }}

select
    winner as fighter,
    count(*) as submission_wins
from {{ ref('stg_fight_results') }}
where method_group = 'Submission'
  and winner is not null
  and winner <> ''
group by winner
order by submission_wins desc
