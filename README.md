# XKCD Data Pipeline

### Abstract

A production-aware data engineering project built for the JustEatTakeway.com case study. This repo consists of the code for the Extraction, Load, Transform (ELT) pipeline that fetches XKCD comic details from the public API, preprocess it with pandas, loads the preprocessed data into PostgreSQL, and transforms it into a Kimball dimensional model using DBT while providing ample data quality checks. All of this processes are orchestarted using Apache Airflow and all the components are wrapped in a Docker Compose file. 

## Table of contents

1. [Architecture Overview](#1-architecture-overview)
2. [Repository Strcutre](#2-repository-structure)
3. [Data Model](#3-data-model)
4. [Prerequisites](#4-prerequisites)
5. [Running the pipeline](#5-running-the-pipeline)
6. [DBT project](#6-dbt-project)
7. [Data Quality checks](#7-data-quality-checks)
8. [Preprocessing Layer](#8-preprocessing-layer)
9. [Airflow DAG](#9-airflow-dag)

## 1. Archutecture Overview

```
XKCD Public API (https://xkcd.com/info.0.json)
    |
    |   
    v
Check for Historical Data
    |
    |   - If raw_xkcd_comics table doesnot exist or empty then backfill
    v
Extraction of XKCD comics
    |
    |   - Utlising Python + requests
    v
Preporocessing Data(pandas)<---------------------------------------------------|
Deduplicate>>Validate Columns>>Stripping Whitespaces>>Date Conversion          | 
    |                                                                          | 
    |                                                                          | 
    v                                                                          | 
Load to PostgreSQL (raw_xkcd_comics)                                           | 
    |                                                                          | 
    |                                                                          | 
    v                                                                          | 
Polling for new comic   --------------------------------------------------------
    |
    |   - Using built in HTTPSensor that check every 1 hour
    v
DBT Transformation
    |
    |-- staging/stg_xkcd_comics (view)
    |-- marts/dim_comics (table)
    |-- marts/dim_date (table)
    |-- marts/fact_xkcd_comics (table)
    |
    v
Data Quality Tests
    - Singular and Generic tests in dbt
```

## 2. Repository Strcutre

```
.
|--- dags/
│   |--- xkcd_dag.py              # Airflow DAG definition
|--- dbt/
│   |--- models/
│   │   |---staging/             # Staging views
|   |   |   |---stg_xkcd_comics.sql # Staging model
|   |   |   |---schema.yml          #Model description and Generic test implemented
|   |   |   |---sources.yml         #registering the source data
│   │   |--- marts/               # Dimensional models
|   |   |    |--- dim_comics.sql  # Dimension model for comic details
|   |   |    |--- dim_date.sql    # Dimension model for date details
|   |   |    |--- fact_xkcd_comics.sql #Fact table containing metrics
|   |   |    |--- schema.yml      #Model descripition and generic test
│   |---tests/                   # Singular tests for data quality
|   |   |---assert_cost_equals_letter_count_times_rate.sql  
|   |   |---assert_no_future_date.sql  
|   |   |---asert_review_in_range.sql  
|   |   |---assert_views_in_range.sql  
|--- scripts/
│   |--- init-airflow-db.sql      # Postgres init script
|--- src/
│   |--- elt_code.py              # Extract & load logic
│   |--- db_utils.py              # DB connection & insert helper
│   |--- secrets_manager.py       # DB credentials
|--- docker-compose.yaml          # All services
|--- README.md
```

## 3. Data Model

Kimball-style star schema with 2 dimensions and 1 fact table:

**dim_comics** - one row per comic
- comic_id (PK)
- title, safe_title, img_url, alt_text, transcript
- published_date
- title_letter_count (used for cost calculation)

**dim_date** - one row per publication date
- date_key (PK, YYYYMMDD)
- published_date, published_year, published_month, published_day
- day_of_week, quarter, day_name, day_of_year, is_weekend

**fact_xkcd_comics** - one row per comic, foreign keys to both dims
- fact_id (PK)
- comic_id (FK -> dim_comics)
- date_key (FK -> dim_date)
- cost_euros (letters * 5)
- views (random 0-10000)
- reviews (random 1.0-10.0)

## 4. Prerequisites

- Docker
- Docker Compose
- Python
- Postgres
- DBT
- Apache Airflow


## 5. Running the pipeline

```bash
# Start everything
docker-compose up -d

# Airflow UI: http://localhost:8080
# Login: airflow / airflow
# pgAdmin: http://localhost:5050
```

The DAG `xkcd_dag` starts paused. Unpause it in the UI and trigger a run. On first run it will detect that `raw_xkcd_comics` is empty and backfill all historical comics automatically. Subsequent runs just fetch the latest comic.

## 6. DBT project

Models are split into staging and marts:

**Staging**
- `stg_xkcd_comics` - clean view on top of raw table, renames columns and builds a proper date

**Marts**
- `dim_comics` - comic attributes + title letter count
- `dim_date` - date dimension with calendar attributes
- `fact_xkcd_comics` - metrics (cost, views, reviews) with FKs to both dims

## 7. Data Quality checks

**Built-in DBT tests:**
- unique, not_null on primary keys
- relationships between fact and dimension tables
- accepted_values for quarter, is_weekend

**Custom tests:**
- `assert_cost_equals_letter_count_times_rate` -- verifies cost = letters * 5
- `assert_no_future_date` -- no comics with published_date > today
- `assert_reviews_in_range` -- reviews between 1.0 and 10.0
- `assert_views_in_range` -- views between 0 and 10000

## 8. Preprocessing Layer

All preprocessing happens in `src/elt_code.py` before loading:

1. **Deduplicate** - drop duplicates on `num` (comic ID)
2. **Validate columns** - ensure expected columns exist, drop unexpected ones
3. **Strip whitespace** - clean text fields, convert empty strings to None
4. **Type conversion** - cast day/month/year to integers

## 9. Airflow DAG

`dags/xkcd_dag.py` runs on a **Mon/Wed/Fri** schedule (`0 0 * * 1,3,5`) with the following task flow:

```
ensure_historical_data
    |
    v
wait_for_new_comic (HttpSensor)
    |
    v
extract_and_load_data
    |
    v
dbt_run
    |
    v
dbt_test
```

- **ensure_historical_data** - checks if raw table has data; if empty, backfills everything
- **wait_for_new_comic** - polls the API every 1 hour to check if today's comic is published
- **extract_and_load_data** - fetches the latest comic and loads to Postgres
- **dbt_run** - builds all models
- **dbt_test**- runs all data quality tests

