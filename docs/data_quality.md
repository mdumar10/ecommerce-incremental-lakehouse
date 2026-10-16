# Data Quality

Data quality checks are applied during the Silver transformations. The aim is to clean ordinary data issues and keep records that violate important business rules out of the Silver tables.

## Cleaning

The transformations perform basic cleanup where needed, including trimming whitespace, removing duplicates using the appropriate keys, and standardizing data.

Bronze tables remain unchanged so the original ingested data is preserved.

## Validation and Quarantine

Records that fail implemented validation rules are written to `ecommerce.quarantine.<table>`. The quarantined records retain their original values and include a reason explaining why they were rejected.

### Orders

The implemented checks identify:

- Delivered orders that have no customer delivery date.
- Orders where the carrier delivery date is earlier than the purchase timestamp.
- Orders where the customer delivery date is earlier than the carrier delivery date.

### Customers

The implemented checks identify:

- Missing customer IDs.
- ZIP codes outside the range of 0–99999.
- Missing state values.

Other tables have their own transformations and cleanup rules. Not every table uses the same validation rules.

## Validation After Pipeline Runs

I checked the row counts and distinct keys of the Gold tables after running the pipeline with generated source changes.

The checks confirmed that each Gold table retained its intended grain: one row per order, customer, or product.

The incremental test also confirmed that 20 new orders and 10 new customers reached the Gold tables without creating duplicate keys.

## Limitations

The profiling notebooks are used to inspect data and investigate issues. They are separate from the scheduled Bronze → Silver → Gold workflow.

The checks documented here describe implemented rules, not a claim that every possible data-quality issue is automatically detected.
