#!/bin/bash

set -euo pipefail

MB_URL=${MB_URL:-"http://localhost:3000"}
MB_EMAIL=${MB_EMAIL:-"reshefsharvit21@gmail.com"}
MB_PASSWORD=${MB_PASSWORD:-"password1!"}
MB_DATABASE=${MB_DATABASE:-"ufc"}
MB_GOAT_SCHEMA=${MB_GOAT_SCHEMA:-"fighters_extracted_goat_status"}

echo $MB_GOAT_SCHEMA

MB_SESSION=$(curl -s -X POST "$MB_URL/api/session" \
                -H "Content-Type: application/json" \
                -d "{\"username\":\"$MB_EMAIL\",\"password\":\"$MB_PASSWORD\"}" | jq -r .id)

if [ -z "$MB_SESSION" ] || [ "$MB_SESSION" = "null" ]; then
  echo "Failed to create a Metabase session."
  exit 1
fi

DB_RESPONSE=$(curl -s "$MB_URL/api/database" \
  -H "X-Metabase-Session: $MB_SESSION")

if ! printf '%s' "$DB_RESPONSE" | jq -e . >/dev/null 2>&1; then
  echo "Failed to parse /api/database response from Metabase:"
  echo "$DB_RESPONSE"
  exit 1
fi

MB_DATABASE_ID=$(printf '%s' "$DB_RESPONSE" | \
  jq -r ".data[] | select(.name == \"$MB_DATABASE\") | .id" | head -n 1)

if [ -z "$MB_DATABASE_ID" ] || [ "$MB_DATABASE_ID" = "null" ]; then
  echo "Failed to find Metabase database named $MB_DATABASE"
  exit 1
fi

IMAGE_VIS_SETTINGS=$(jq -c -n '{
  column_settings: {
    (["ref", ["field","fighter_image_url", {"base-type":"type/Text"}]] | tojson): {
      "table.cell_display": "image"
    },
    (["name", "fighter_image_url"] | tojson): {
      "table.cell_display": "image"
    }
  }
}')

create_card() {
  local name="$1"
  local query="$2"

  curl -s -X POST "$MB_URL/api/card" \
    -H "X-Metabase-Session: $MB_SESSION" \
    -H "Content-Type: application/json" \
    -d "$(jq -n \
      --arg name "$name" \
      --arg query "$query" \
      --argjson db "$MB_DATABASE_ID" \
      --argjson vis "$IMAGE_VIS_SETTINGS" \
      '{
        name: $name,
        dataset_query: {
          type: "native",
          native: {
            query: $query,
            "template-tags": {}
          },
          database: $db
        },
        display: "table",
        visualization_settings: $vis
      }')"
}

apply_image_settings_to_card() {
  local card_id="$1"
  local card_json
  local has_table
  local new_vis
  local field_key
  local name_key
  local payload

  card_json=$(curl -s "$MB_URL/api/card/$card_id" \
    -H "X-Metabase-Session: $MB_SESSION")

  has_table=$(printf '%s' "$card_json" | jq -r '.display == "table"')
  if [ "$has_table" != "true" ]; then
    return 0
  fi

  field_key=$(jq -n '["ref", ["field","fighter_image_url", {"base-type":"type/Text"}]] | tojson')
  name_key=$(jq -n '["name", "fighter_image_url"] | tojson')

  new_vis=$(printf '%s' "$card_json" | jq -c --argjson vis "$IMAGE_VIS_SETTINGS" --arg field_key "$field_key" --arg name_key "$name_key" '
    (.visualization_settings // {}) as $current
    | ($current.column_settings // {}) as $cols
    | $cols
        | if .[$field_key]? and (.[ $field_key ].column_settings? != null) then
            .[$field_key] = .[$field_key].column_settings
          else . end
        | if .[$name_key]? and (.[ $name_key ].column_settings? != null) then
            .[$name_key] = .[$name_key].column_settings
          else . end
        | .[$field_key] = {"table.cell_display": "image"}
        | .[$name_key] = {"table.cell_display": "image"}
    | $current + { column_settings: . }
  ')

  payload=$(printf '%s' "$card_json" | jq -c --argjson vis "$new_vis" '{
    name,
    dataset_query,
    display,
    description,
    collection_id,
    visualization_settings: $vis
  }')

  curl -s -X PUT "$MB_URL/api/card/$card_id" \
    -H "X-Metabase-Session: $MB_SESSION" \
    -H "Content-Type: application/json" \
    -d "$payload" >/dev/null
}

apply_image_settings_to_all_cards() {
  local cards_json
  local card_ids

  cards_json=$(curl -s "$MB_URL/api/card?f=all" \
    -H "X-Metabase-Session: $MB_SESSION")

  card_ids=$(printf '%s' "$cards_json" | jq -r '
    if type == "object" and (.data? != null) then
      .data[]?.id
    elif type == "array" then
      .[]?.id
    else
      empty
    end
  ')
  if [ -z "$card_ids" ]; then
    return 0
  fi

  while read -r card_id; do
    if [ -n "$card_id" ]; then
      apply_image_settings_to_card "$card_id"
    fi
  done <<< "$card_ids"
}

QUERY=$(cat <<'SQL'
SELECT
  fighter,
  case
    when fighter is null or fighter = '' then null
    else concat(
      'http://localhost:8888/',
      regexp_replace(
        regexp_replace(lower(fighter), '[^a-z0-9]+', '_', 'g'),
        '^_+|_+$',
        '',
        'g'
      ),
      '.png'
    )
  end as fighter_image_url,
  avg_opponent_win_pct_at_time
FROM fighters_extracted_goat_status.mv_quality_of_opposition_faced
ORDER BY avg_opponent_win_pct_at_time DESC NULLS LAST
LIMIT 25;
SQL
)
create_card "Quality of Opposition (At Time) - Top 25" "$QUERY"

QUERY=$(cat <<'SQL'
SELECT
  fighter,
  case
    when fighter is null or fighter = '' then null
    else concat(
      'http://localhost:8888/',
      regexp_replace(
        regexp_replace(lower(fighter), '[^a-z0-9]+', '_', 'g'),
        '^_+|_+$',
        '',
        'g'
      ),
      '.png'
    )
  end as fighter_image_url,
  avg_opponent_career_wins
FROM fighters_extracted_goat_status.mv_quality_of_opposition_faced
ORDER BY avg_opponent_career_wins DESC NULLS LAST
LIMIT 25;
SQL
)
create_card "Quality of Opposition (Career Wins) - Top 25" "$QUERY"

QUERY=$(cat <<'SQL'
SELECT
  fighter,
  case
    when fighter is null or fighter = '' then null
    else concat(
      'http://localhost:8888/',
      regexp_replace(
        regexp_replace(lower(fighter), '[^a-z0-9]+', '_', 'g'),
        '^_+|_+$',
        '',
        'g'
      ),
      '.png'
    )
  end as fighter_image_url,
  category,
  title_fight_wins
FROM fighters_extracted_goat_status.mv_title_fight_results_by_fighter
WHERE title_fight_wins > 0
ORDER BY title_fight_wins DESC
LIMIT 50;
SQL
)
create_card "Title Fight Wins by Fighter and Category" "$QUERY"

QUERY=$(cat <<'SQL'
SELECT
  fighter,
  case
    when fighter is null or fighter = '' then null
    else concat(
      'http://localhost:8888/',
      regexp_replace(
        regexp_replace(lower(fighter), '[^a-z0-9]+', '_', 'g'),
        '^_+|_+$',
        '',
        'g'
      ),
      '.png'
    )
  end as fighter_image_url,
  distinct_title_categories
FROM fighters_extracted_goat_status.mv_multiple_weight_class_champs
ORDER BY distinct_title_categories DESC, total_title_wins DESC;
SQL
)
create_card "Multiple Division Champs" "$QUERY"

QUERY=$(cat <<'SQL'
SELECT
  fighter,
  case
    when fighter is null or fighter = '' then null
    else concat(
      'http://localhost:8888/',
      regexp_replace(
        regexp_replace(lower(fighter), '[^a-z0-9]+', '_', 'g'),
        '^_+|_+$',
        '',
        'g'
      ),
      '.png'
    )
  end as fighter_image_url,
  wins_over_champions
FROM fighters_extracted_goat_status.mv_wins_over_champions_agg
ORDER BY wins_over_champions DESC
LIMIT 25;
SQL
)
create_card "Wins Over Champions (Distinct Opponents)" "$QUERY"

QUERY=$(cat <<'SQL'
SELECT
  fighter,
  case
    when fighter is null or fighter = '' then null
    else concat(
      'http://localhost:8888/',
      regexp_replace(
        regexp_replace(lower(fighter), '[^a-z0-9]+', '_', 'g'),
        '^_+|_+$',
        '',
        'g'
      ),
      '.png'
    )
  end as fighter_image_url,
  longest_win_streak
FROM fighters_extracted_goat_status.mv_fighters_by_longest_winning_streak
ORDER BY longest_win_streak DESC
LIMIT 25;
SQL
)
create_card "Longest Winning Streaks (10+)" "$QUERY"

QUERY=$(cat <<'SQL'
SELECT
  fighter,
  case
    when fighter is null or fighter = '' then null
    else concat(
      'http://localhost:8888/',
      regexp_replace(
        regexp_replace(lower(fighter), '[^a-z0-9]+', '_', 'g'),
        '^_+|_+$',
        '',
        'g'
      ),
      '.png'
    )
  end as fighter_image_url,
  win_pct
FROM fighters_extracted_goat_status.mv_fighters_best_record_min_10_fights
ORDER BY win_pct DESC
LIMIT 25;
SQL
)
create_card "Best Records (Min 10 Fights)" "$QUERY"

QUERY=$(cat <<'SQL'
SELECT
  fighter,
  case
    when fighter is null or fighter = '' then null
    else concat(
      'http://localhost:8888/',
      regexp_replace(
        regexp_replace(lower(fighter), '[^a-z0-9]+', '_', 'g'),
        '^_+|_+$',
        '',
        'g'
      ),
      '.png'
    )
  end as fighter_image_url,
  non_decision_wins
FROM fighters_extracted_goat_status.mv_fighters_by_non_decision_wins
ORDER BY non_decision_wins DESC
LIMIT 25;
SQL
)
create_card "Non-Decision Wins" "$QUERY"

QUERY=$(cat <<'SQL'
SELECT
  fighter,
  case
    when fighter is null or fighter = '' then null
    else concat(
      'http://localhost:8888/',
      regexp_replace(
        regexp_replace(lower(fighter), '[^a-z0-9]+', '_', 'g'),
        '^_+|_+$',
        '',
        'g'
      ),
      '.png'
    )
  end as fighter_image_url,
  weight_category,
  title_defenses
FROM fighters_extracted_goat_status.mv_title_defenses
ORDER BY title_defenses DESC
LIMIT 50;
SQL
)
create_card "Title Defenses by Category" "$QUERY"

QUERY=$(cat <<'SQL'
SELECT
  fighter,
  case
    when fighter is null or fighter = '' then null
    else concat(
      'http://localhost:8888/',
      regexp_replace(
        regexp_replace(lower(fighter), '[^a-z0-9]+', '_', 'g'),
        '^_+|_+$',
        '',
        'g'
      ),
      '.png'
    )
  end as fighter_image_url,
  title_fights,
  championship_rounds_fought
FROM fighters_extracted_goat_status.mv_championship_rounds_fought
ORDER BY championship_rounds_fought DESC, title_fights DESC, fighter
LIMIT 50;
SQL
)
create_card "Championship Rounds Fought (Min 5 Title Fights)" "$QUERY"

QUERY=$(cat <<'SQL'
SELECT
  fighter,
  case
    when fighter is null or fighter = '' then null
    else concat(
      'http://localhost:8888/',
      regexp_replace(
        regexp_replace(lower(fighter), '[^a-z0-9]+', '_', 'g'),
        '^_+|_+$',
        '',
        'g'
      ),
      '.png'
    )
  end as fighter_image_url,
  weight_category,
  max_consecutive_title_defenses
FROM fighters_extracted_goat_status.mv_consecutive_title_defenses
ORDER BY max_consecutive_title_defenses DESC, fighter, weight_category
LIMIT 50;
SQL
)
create_card "Consecutive Title Defenses by Category" "$QUERY"

QUERY=$(cat <<'SQL'
SELECT
  fighter,
  case
    when fighter is null or fighter = '' then null
    else concat(
      'http://localhost:8888/',
      regexp_replace(
        regexp_replace(lower(fighter), '[^a-z0-9]+', '_', 'g'),
        '^_+|_+$',
        '',
        'g'
      ),
      '.png'
    )
  end as fighter_image_url,
  clutch_wins
FROM fighters_extracted_goat_status.mv_clutch_wins_min_10_fights
ORDER BY clutch_wins DESC
LIMIT 25;
SQL
)
create_card "Clutch Wins (Min 10 Fights)" "$QUERY"

apply_image_settings_to_all_cards
