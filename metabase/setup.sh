#!/bin/bash

set -euo pipefail

MB_URL=${MB_URL:-"http://localhost:3000"}
MB_EMAIL=${MB_EMAIL:-"reshefsharvit21@gmail.com"}
MB_PASSWORD=${MB_PASSWORD:-"password1!"}
MB_FIRST_NAME=${MB_FIRST_NAME:-"Reshef"}
MB_LAST_NAME=${MB_LAST_NAME:-"Sharvit"}

DB_NAME=${DB_NAME:-"ufc"}
DB_USER=${DB_USER:-"postgres"}
DB_PASSWORD=${DB_PASSWORD:-"postgres"}
DB_HOST=${DB_HOST:-"host.docker.internal"}
DB_PORT=${DB_PORT:-"5432"}

is_json() {
  case "$1" in
    \{*|\[*) return 0 ;;
    *) return 1 ;;
  esac
}

SETUP_RESPONSE=$(curl -s "$MB_URL/api/setup")
if is_json "$SETUP_RESPONSE"; then
  SETUP_STATE=$(printf '%s' "$SETUP_RESPONSE" | jq -r 'if type == "object" then .state // "unknown" else "unknown" end')
  SETUP_TOKEN=$(printf '%s' "$SETUP_RESPONSE" | jq -r 'if type == "object" then .token else empty end')
else
  SETUP_STATE="unknown"
  SETUP_TOKEN=""
fi

if [ -z "$SETUP_TOKEN" ] || [ "$SETUP_TOKEN" = "null" ]; then
  PROPS_RESPONSE=$(curl -s "$MB_URL/api/session/properties")
  if is_json "$PROPS_RESPONSE"; then
    SETUP_STATE=$(printf '%s' "$PROPS_RESPONSE" | jq -r 'if type == "object" then (if .setup == true then "setup-complete" else "unknown" end) else "unknown" end')
    SETUP_TOKEN=$(printf '%s' "$PROPS_RESPONSE" | jq -r 'if type == "object" then ."setup-token" else empty end')
  fi
fi

if [ "$SETUP_STATE" = "setup-complete" ]; then
  MB_SESSION=$(curl -s -X POST "$MB_URL/api/session" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"$MB_EMAIL\",\"password\":\"$MB_PASSWORD\"}" | jq -r .id)

  if [ -z "$MB_SESSION" ] || [ "$MB_SESSION" = "null" ]; then
    echo "Metabase setup already complete, but login failed."
    exit 1
  fi
else
  if [ -z "$SETUP_TOKEN" ] || [ "$SETUP_TOKEN" = "null" ]; then
    echo "Failed to obtain setup token from Metabase. Is it running at $MB_URL?"
    exit 1
  fi

  curl -s -X POST "$MB_URL/api/setup" \
    -H "Content-Type: application/json" \
    -d "{\"token\":\"$SETUP_TOKEN\",\"user\":{\"email\":\"$MB_EMAIL\",\"password\":\"$MB_PASSWORD\",\"first_name\":\"$MB_FIRST_NAME\",\"last_name\":\"$MB_LAST_NAME\"},\"prefs\":{\"site_name\":\"UFC Warehouse\",\"allow_tracking\":false},\"database\":{\"name\":\"$DB_NAME\",\"engine\":\"postgres\",\"details\":{\"host\":\"$DB_HOST\",\"port\":$DB_PORT,\"dbname\":\"$DB_NAME\",\"user\":\"$DB_USER\",\"password\":\"$DB_PASSWORD\",\"ssl\":false}}}" > /dev/null

  MB_SESSION=$(curl -s -X POST "$MB_URL/api/session" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"$MB_EMAIL\",\"password\":\"$MB_PASSWORD\"}" | jq -r .id)
fi

if [ -z "$MB_SESSION" ] || [ "$MB_SESSION" = "null" ]; then
  echo "Failed to create a Metabase session."
  exit 1
fi

DB_EXISTS=$(curl -s "$MB_URL/api/database" \
  -H "X-Metabase-Session: $MB_SESSION" | \
  jq -r ".data[] | select(.name == \"$DB_NAME\") | .id" | head -n 1)

if [ -z "$DB_EXISTS" ] || [ "$DB_EXISTS" = "null" ]; then
  curl -s -X POST "$MB_URL/api/database" \
    -H "X-Metabase-Session: $MB_SESSION" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"$DB_NAME\",\"engine\":\"postgres\",\"details\":{\"host\":\"$DB_HOST\",\"port\":$DB_PORT,\"dbname\":\"$DB_NAME\",\"user\":\"$DB_USER\",\"password\":\"$DB_PASSWORD\",\"ssl\":false}}" > /dev/null
fi

echo "Metabase setup complete."
