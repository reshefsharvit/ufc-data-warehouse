import csv
from pathlib import Path
from typing import Optional

import dlt


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "scrape_ufc_stats-main"
VACANCY_CSV = (
    ROOT_DIR
    / "scripts"
    / "vacancy_and_strips_scraper"
    / "output"
    / "title_vacancies.csv"
)
VACANCY_TABLE = "title_status_changes_outside_octagon"
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


def csv_resource(path: Path, name_override: Optional[str] = None):
    resource_name = name_override or table_name(path)

    @dlt.resource(name=resource_name, write_disposition="replace")
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
    if not VACANCY_CSV.exists():
        raise SystemExit(
            f"Missing vacancy export at {VACANCY_CSV}. "
            "Run scripts/vacancy_and_strips_scraper/extract_vacancies.py first."
        )

    pipeline = dlt.pipeline(
        pipeline_name="ufc_csv",
        destination="postgres",
        dataset_name="ufc",
    )

    resources = [csv_resource(path) for path in csv_files]
    resources.append(csv_resource(VACANCY_CSV, name_override=VACANCY_TABLE))
    load_info = pipeline.run(resources)
    print(load_info)


if __name__ == "__main__":
    main()
