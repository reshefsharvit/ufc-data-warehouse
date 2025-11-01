{{ config(
    materialized='table',
    schema='fighters_extracted'
) }}

with src as (
    select
        fighter_id,
        document                                        as about_json,
        document->>'id'                                 as about_id,
        document->>'name'                               as name,
        document->>'nickname'                           as nickname,
        document->>'division'                           as division,
        document->>'gender'                             as gender,
        document->>'Status'                             as status,
        document->>'Place of Birth'                     as place_of_birth,
        document->>'Trains at'                          as trains_at,
        document->>'Fighting style'                     as fighting_style,
        nullif(document->>'Age','')::int                as age,
        nullif(document->>'Height','')::numeric         as height_in,
        nullif(document->>'Weight','')::numeric         as weight_lb,
        nullif(document->>'Reach','')::numeric          as reach_in,
        nullif(document->>'Leg reach','')::numeric      as leg_reach_in,
        case
            when document ? 'Octagon Debut'
            then to_date(
                regexp_replace(document->>'Octagon Debut', '\.', '', 'g'),
                'Mon DD, YYYY'
            )
        end                                             as octagon_debut
    from {{ source('fighters_data','fighters') }}
)
select * from src
