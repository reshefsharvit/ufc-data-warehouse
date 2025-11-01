# UFC data warehouse using dlt, dbt and PostgreSQL.

## What's in here?
This project loads fighters and fights stats, transforms and creates views for various stat lines.

## Acknowledgements
This project uses a data set published by [Tawhid Monowar](https://huggingface.co/datasets/tawhidmonowar/ufc-fighters-stats-and-records-dataset).  
Many thanks to him for making this resource publicly available!


## Installation
1. Start a PostgreSQL docker container
```bash
docker run -it -d \
  --name ufcpg \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=ufc \
  -p 5432:5432 \
  postgres:16
```  

2. Download the dataset
```bash
wget --content-disposition 'https://huggingface.co/datasets/tawhidmonowar/ufc-fighters-stats-and-records-dataset/resolve/main/ufc_fighters_stats_and_records.json?download=true'
```


3. Create a virtual env and install dlt and dby:
```shell
python3 -m venv venv
source venv/bin/activate

pip install dbt-postgres dlt[postgres]
```

## Execution
### 1. Load JSON data to the database
```shell
python3 dlt/load.py
```

### 2. Transform and build tables and views
```shell
cd ufc
dbt debug
dbt deps
dbt seed
dbt run
```
