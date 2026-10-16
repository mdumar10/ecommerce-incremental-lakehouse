# Architecture

The project uses a Bronze, Silver and Gold pipeline on Databricks. Source data comes from PostgreSQL on Supabase.

## Data Flow

`Supabase → Bronze → Silver → Gold`

### Bronze

Bronze loads source tables into Delta tables. The load type is defined in `config/tables.yml`.

- Incremental tables use an `updated_at` watermark and Delta MERGE.
- Static tables are loaded when their Bronze tables are first created and are skipped on subsequent runs.

Static tables are not automatically refreshed if their source data changes.

### Silver

Silver reads the Bronze tables and applies the required transformations and data-quality checks.

Records that fail the implemented validation rules are written to `ecommerce.quarantine.<table>`, along with a reason. Bronze remains unchanged.

### Gold

Gold reads the processed data and builds three analytical tables:

- `fact_orders`
- `customer_summary`
- `product_summary`

The Gold tables are fully overwritten on each run. Child tables are aggregated before joining where necessary to preserve the intended table grain.

## Orchestration

The workflow is managed through a Databricks Job defined in `resources/job.yml`.

The tasks run in this order:

1. `notebooks/jobs/bronze.py`
2. `notebooks/jobs/silver.py`
3. `notebooks/jobs/gold.py`

These files are entry points for the workflow. The actual processing logic is kept in the `pipeline/` directory.

`databricks.yml` configures the Databricks Asset Bundle. Running `databricks bundle deploy` creates or updates the resources in Databricks. The deployed Job can then be run with `databricks bundle run ecommerce_pipeline`.

The profiling notebooks are used separately to inspect data and validate results; they are not part of the scheduled pipeline tasks.
