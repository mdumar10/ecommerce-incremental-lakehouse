# Olist E-commerce Lakehouse

I built this project to practise an end-to-end data engineering pipeline using PostgreSQL, Databricks, PySpark and Delta Lake.

The pipeline loads e-commerce data from Supabase into Bronze, cleans and validates it in Silver, and builds analytical tables in Gold. I also set up a Databricks Job to run the three layers in order.

## Pipeline

`Supabase / PostgreSQL → Bronze → Silver → Gold`

**Bronze — ingestion**

Source tables are loaded into Delta tables. Incremental tables use an `updated_at` watermark and Delta MERGE to insert new records and update existing ones. Static tables are loaded when their Bronze tables are first created.

**Silver — cleaning and data quality**

The transformations handle things such as whitespace, duplicate records and table-specific data-quality rules. Invalid records are written to quarantine tables with a reason, while Bronze data remains unchanged.

**Gold — analytical tables**

Gold contains three tables:

| Table | Grain |
|---|---|
| `fact_orders` | One row per order |
| `customer_summary` | One row per customer |
| `product_summary` | One row per product |

For `fact_orders`, I aggregate order items, payments and reviews before joining them to orders. This prevents one order from appearing multiple times because of one-to-many joins.

Gold tables are rebuilt using full overwrite on each pipeline run.

## How to run the pipeline

The pipeline runs on Databricks. You need a Databricks workspace, the Databricks CLI, access to the PostgreSQL source database, and the required secrets configured in Databricks.

### 1. Configure the connection

The pipeline reads its PostgreSQL connection details from a Databricks secret scope named `supabase`.

The scope needs these keys:

- `SUPABASE_HOST`
- `SUPABASE_PORT`
- `SUPABASE_DATABASE`
- `SUPABASE_USER`
- `SUPABASE_PASSWORD`

The source tables must exist, and the Databricks compute must be able to connect to PostgreSQL. Before deploying to a different workspace, update the workspace host in `databricks.yml`.

### 2. Validate and deploy

Run these commands from the project root:

```bash
databricks bundle validate
databricks bundle deploy
```

The first command checks the bundle configuration. The second creates or updates the Databricks resources defined in the project.

### 3. Run the pipeline

```bash
databricks bundle run ecommerce_pipeline
```

The Databricks Job runs these tasks in order:

1. `notebooks/jobs/bronze.py`
2. `notebooks/jobs/silver.py`
3. `notebooks/jobs/gold.py`

The task dependencies are defined in `resources/job.yml`, and `databricks.yml` configures the Asset Bundle.

After changing the deployed code or job configuration, deploy the bundle again before running it.

## Incremental loading test

I used `generator/data_generator.py` to test the pipeline with new and updated source records. The generator created 20 orders, created 10 customers, and updated 10 existing orders.

After running the Databricks Job, I checked the Gold row counts and distinct keys.

| Gold table | Before | After | Change |
|---|---:|---:|---:|
| `fact_orders` | 99,611 | 99,631 | +20 |
| `customer_summary` | 99,466 | 99,476 | +10 |
| `product_summary` | 32,945 | 32,945 | 0 |

The distinct key counts matched the row counts for all three tables after the run.

This test showed that the new records reached Gold without breaking the intended table grains. Gold itself uses full refresh, so this validates the end-to-end result rather than an incremental Gold write.

**Optional: run the data generator**

The generator writes to PostgreSQL. Use a development or test database, not a database you cannot safely modify.

Create a `.env` file in the project root with the five `SUPABASE_*` connection variables listed above. Do not commit this file or put real credentials in GitHub.

Install the generator's local dependencies and run it from the project root:

```bash
python -m pip install psycopg2-binary PyYAML python-dotenv
python generator/data_generator.py
```

The generated data is written to the source database. Run the Databricks Job afterwards to process the changes.

## Project structure

```text
config/
  config.yaml
  tables.yml

generator/
  data_generator.py

notebooks/
  jobs/          # Databricks Job entry points
  profiling/     # Separate profiling notebooks

pipeline/
  bronze.py
  silver/        # Silver transformations and quality rules
  gold/          # Gold analytical tables

resources/
  job.yml

databricks.yml
requirements.txt
```

## Technologies

- Python and PySpark
- Apache Spark and Delta Lake
- PostgreSQL on Supabase
- Databricks Jobs and Asset Bundles

## Current scope

This is a batch lakehouse project, not a streaming or CDC system. Gold tables are fully rebuilt on each run, and static Bronze tables are not automatically refreshed after their initial load.

The incremental test and table-grain checks have been run successfully. Automated test coverage and some supporting documentation still need further work.
