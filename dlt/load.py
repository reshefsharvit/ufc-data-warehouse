import dlt
import json

ABOUT_COLUMNS = {
    "fighter_id": {"data_type": "text"},
    "document":   {"data_type": "json"}   # -> JSONB in Postgres
}

FIGHTS_COLUMNS = {
    "fighter_id": {"data_type": "text"},
    "fight_history": {"data_type": "json"}  # -> JSONB in Postgres
}

@dlt.resource(
    name="fighters",                       # table: fighters_data.fighters
    write_disposition="replace",
    columns=ABOUT_COLUMNS,
    primary_key="fighter_id"               # optional but handy
)
def load_fighters_about():
    with open("ufc_fighters_stats_and_records.json", "r") as f:
        data = json.load(f)

    for item in data:
        about = item.get("about")
        if about:
            yield {
                "fighter_id": about.get("id"),
                "document": about
            }

@dlt.resource(
    name="fighter_fight_history",          # table: fighters_data.fighter_fight_history
    write_disposition="replace",
    columns=FIGHTS_COLUMNS,
    primary_key="fighter_id"
)
def load_fighters_fight_history():
    with open("ufc_fighters_stats_and_records.json", "r") as f:
        data = json.load(f)

    for item in data:
        about = item.get("about") or {}
        fighter_id = about.get("id")
        history = item.get("fight_history")
        # keep an empty {} if present but empty; skip only if missing fighter_id entirely
        if fighter_id is not None and history is not None:
            yield {
                "fighter_id": fighter_id,
                "fight_history": history
            }

pipeline = dlt.pipeline(
    pipeline_name="fighter_pipeline",
    destination="postgres",
    dataset_name="fighters_data"
)

# Run both resources in one go
info = pipeline.run([load_fighters_about(), load_fighters_fight_history()])
print(info)
