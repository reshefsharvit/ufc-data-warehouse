import csv
from pathlib import Path

import dlt


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "scrape_ufc_stats-main"
FACT_TABLES = {
    "ufc_fight_details",
    "ufc_fight_results",
    "ufc_fight_stats",
}


def table_name(path: Path) -> str:
    stem = path.stem
    if stem in FACT_TABLES:
        return f"fact_{stem}"
    return f"dim_{stem}"


def csv_resource(path: Path):
    @dlt.resource(name=table_name(path), write_disposition="replace")
    def _resource():
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                yield row

    return _resource


def main() -> None:
    csv_files = sorted(DATA_DIR.glob("*.csv"))
    if not csv_files:
        raise SystemExit(f"No CSV files found in {DATA_DIR}")

    pipeline = dlt.pipeline(
        pipeline_name="ufc_csv",
        destination="postgres",
        dataset_name="ufc",
    )

    resources = [csv_resource(path) for path in csv_files]
    load_info = pipeline.run(resources)
    print(load_info)


if __name__ == "__main__":
    main()
