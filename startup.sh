#!/usr/bin/env bash
set -euo pipefail

# 1. Start a PostgreSQL docker container
docker run -it -d \
  --name ufcpg \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=ufc \
  -p 5432:5432 \
  postgres:16

# 2. Download the dataset
wget https://github.com/Greco1899/scrape_ufc_stats/archive/refs/heads/main.zip
unzip main.zip "*.csv"

# 3. scrape title vancancies, strips, injures, etc from Wikipedia
python3 scripts/vacancy_and_strips_scraper/extract_vacancies.py

# 4. Create a virtual env and install dlt and dbt
python3 -m venv venv
source venv/bin/activate

pip install dbt-postgres dlt[postgres]

# 5. Load JSON data to the database
python3 dlt/load.py

# 6. Transform and build tables and views
cd ufc
dbt debug
dbt deps
dbt seed
dbt run
cd ..

# 7. Start Metabase in Docker
docker run -it -d \
  --name metabase \
  -p 3000:3000 \
  metabase/metabase

# need to allow metabase to fully initialize
echo "allow metabase to fully initialize"
sleep 15

# 8. Initialize Metabase and create the admin user programmatically
./metabase/setup.sh

sleep 5;

# 9. Create the Metabase charts
./metabase/charts.sh > /dev/null

# 10. Serve the local fighter images over HTTP
python3 images/serve_images.py > logs/serve_images.log 2>&1 &
