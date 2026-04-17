# Unity Catalog — Catalog Migration Notebook

Migrates a Unity Catalog catalog that was created **without a managed storage location** to a new catalog with proper managed storage, preserving the original catalog name so downstream workloads are unaffected.

## What It Does

The notebook follows a create-migrate-swap pattern:

0. **Preflight** — Verifies runner identity and required privileges (catalog ownership, `CREATE CATALOG`, `CREATE MANAGED STORAGE`), resolves the external location that backs `managed_storage_location`, checks that `<catalog>_aux` and `<catalog>_bkp` don't already exist, and validates the SQL warehouse is reachable. Hard-fails with a consolidated report before any work is done.
1. **Inventory & Dry Run** — Discovers all objects, detects risks (Delta Sharing, DLT pipelines, out-of-scope objects), and reports what will be migrated.
2. **Create `<catalog>_aux`** — New catalog with the specified managed storage location.
3. **Migrate Schemas** — Recreates all schemas (including empty ones) with comments, properties, grants, and ownership.
4. **Migrate Managed Tables** — `DEEP CLONE` each table, then restores: table/column comments, tags, PK/FK constraints, grants, and ownership.
5. **Migrate Functions** — Recreates SQL and Python UDFs from `information_schema` metadata.
6. **Migrate Managed Volumes** — Recreates volumes and copies files via `dbutils.fs.cp`.
7. **Capture View Definitions** — Stores DDL for views and materialized views before the swap.
8. **Rename Swap** — `<catalog>` → `<catalog>_bkp`, then `<catalog>_aux` → `<catalog>`.
9. **Recreate Views & Materialized Views** — Uses original DDL (catalog references resolve to the new catalog post-swap). Views are created with a retry loop to handle dependency ordering.
10. **Transfer Catalog Ownership** — Done last so the notebook runner retains admin access throughout.
11. **Validation** — Compares object counts, row counts, and grants between the migrated and backup catalogs.

## Parameters

| Parameter | Description | Example |
|---|---|---|
| `catalog_name` | Name of the source catalog to migrate | `prod_analytics` |
| `managed_storage_location` | S3 path for the new catalog's managed storage | `s3://my-bucket/uc/prod_analytics` |
| `log_catalog` | Catalog where the migration log table will be created | `migration_ops` |
| `sql_warehouse_id` | SQL warehouse ID for materialized view operations (required if MVs exist) | `01370556fad60fda` |
| `dry_run` | `true` = inventory only, no changes; `false` = execute migration | `true` |

## Prerequisites

### Permissions

The preflight cell checks all of the following and hard-fails if any are missing. **Metastore admins satisfy every check implicitly** — if the runner is a metastore admin, the ownership / `CREATE CATALOG` / `CREATE MANAGED STORAGE` checks auto-pass without needing explicit grants.

| Scope | Privilege | Why |
|---|---|---|
| Source catalog | **Ownership** OR metastore admin | Required for `ALTER CATALOG ... RENAME` during cutover. |
| Source catalog | **`USE CATALOG`** | List schemas and objects. |
| Metastore | **`CREATE CATALOG`** OR metastore admin | Create the `<catalog>_aux` catalog. |
| External location | **`CREATE MANAGED STORAGE`** OR metastore admin | Bind the new catalog to the managed storage path. |

How metastore admin is detected: the preflight calls `WorkspaceClient.metastores.summary()` to read the metastore owner, then compares against the runner's identity and direct group memberships (via `current_user.me()`). If the check is inconclusive (e.g., group membership isn't visible), the ownership check downgrades to a `WARN` rather than a `FAIL`, so admins aren't blocked.

**Metastore admin bootstrap — self-grant:** Metastore admins have implicit admin-level privileges (rename, drop, grant) but data-plane privileges (`USE CATALOG`, `SELECT`, `READ VOLUME`) are evaluated separately and can still be missing. When the preflight confirms the runner is a metastore admin, it automatically issues `GRANT ALL PRIVILEGES ON CATALOG <source> TO <runner>` and the same on the matched external location. These grants are transient — the source catalog becomes `<source>_bkp` after the rename swap, so they do not affect the final migrated catalog. They can be revoked (or the `_bkp` catalog dropped) after validation.

Additional recommendations:

- **Metastore admin** (strongly recommended) — makes ownership transfer, grant reads, and function definition reads trouble-free.
- If not metastore admin, the runner must be the **owner** of every function to read its definition (otherwise `routine_definition` is NULL in `information_schema.routines`).
- The external location that covers `managed_storage_location` must already exist. The preflight auto-resolves it by matching path prefixes.
- The runner also needs `USE CATALOG` + `CREATE SCHEMA` + `CREATE TABLE` on `log_catalog` (verified implicitly — the log table is created at the top of the notebook).

### Environment

- **Databricks Runtime**: Latest LTS (DBR 14.x or newer recommended).
- **Cloud**: AWS (S3 storage paths).
- The **external location** for the managed storage must already exist and be accessible.
- The catalogs `<catalog>_aux` and `<catalog>_bkp` must **not** already exist (the dry run checks for this).

### Before Running

1. **Run in dry-run mode first** (`dry_run = true`) — this also runs the full preflight, so any missing privileges or name collisions surface before the actual migration.
2. **Schedule during a low-activity window** — the rename swap creates a brief moment where the original catalog name doesn't exist.
3. **Pause any DLT pipelines** that target the source catalog to avoid partial writes during migration.
4. **Notify stakeholders** if Delta Sharing is in use — share references will need manual updates.

## How to Run

1. Import the notebook into your Databricks workspace.
2. Attach it to a cluster running the latest LTS DBR.
3. Set the parameters:
   - `catalog_name` = the catalog to migrate
   - `managed_storage_location` = S3 path for the new managed storage
   - `log_catalog` = catalog where the log table will live (e.g., `migration_ops`)
   - `sql_warehouse_id` = ID of a running SQL warehouse (required if you have materialized views)
   - `dry_run` = `true`
4. **Run all cells** — review the inventory report, warnings, and dry-run summary.
5. If everything looks correct, change `dry_run` to `false` and **run all cells** again to execute the migration.

## How to Validate

The notebook runs automated validation as its final phase:

- **Object count comparison** — schemas, tables, views, MVs, functions, volumes
- **Row count comparison** — every managed table is compared row-by-row
- **Grant comparison** — catalog-level and schema-level grants are compared

### Manual Validation (Recommended)

After the notebook completes:

1. **Check dashboards** that query the catalog — they should work without changes.
2. **Trigger dependent workflows** and verify they succeed.
3. **Spot-check table data** — query a few tables and verify the data looks correct.
4. **Verify ownership** — `DESCRIBE TABLE EXTENDED` on a few objects to confirm owners match.
5. **Test a view** — query a view to confirm it resolves correctly.

## Assumptions

- **External tables are out of scope.** They reference external storage and are not migrated by this notebook. They remain in the backup catalog.
- **Streaming tables are out of scope.** They cannot be cloned and require full re-ingestion from source. They remain in the backup catalog.
- **ML models (Unity Catalog registered models) are out of scope.** Use `mlflow.copy_model_version()` if needed.
- **All volumes in the source catalog are managed.** External volumes are not migrated.
- **The notebook is not idempotent.** If it fails midway, you may need to clean up the `_aux` catalog and re-run from scratch.
- **The notebook runner has metastore admin privileges** for full access to object definitions and ownership transfer.

## Limitations & Risks

### Downtime During Rename Swap

There is a brief window between the two `ALTER CATALOG RENAME` operations where the original catalog name does not exist. Any query or workload referencing the catalog during this window will fail. **Run during a low-activity period.**

### Materialized Views Require a SQL Warehouse and Full Recomputation

Materialized views cannot be cloned. They are recreated in the new catalog after the rename swap, which triggers a full recomputation via a serverless pipeline. Key details:

- **A SQL warehouse is required.** MV operations (DDL capture, creation, and ownership transfer) cannot run on general-purpose compute — MV ownership changes are processed by DLT's `ChangeDltDatasetOwnerHandler`, which denies `ALTER OWNER` from general compute even for the MV creator. Provide the `sql_warehouse_id` parameter pointing to a running Serverless or Pro SQL warehouse. If omitted, MVs will be skipped.
- **Internal backing tables** (`__materialization_*`, `event_log_*`) created by the MV pipeline are automatically excluded from DEEP CLONE. They are regenerated when the MV is recreated.
- For large or complex materialized views, recomputation may take significant time and compute resources.

### Delta Sharing

If any **shares** reference tables in the migrated catalog, those references will point to the backup catalog (`_bkp`) after migration. You must manually update shares:

```sql
-- Remove old reference
ALTER SHARE my_share REMOVE TABLE <catalog>_bkp.schema.table;

-- Add new reference
ALTER SHARE my_share ADD TABLE <catalog>.schema.table;
```

### DLT / Lakeflow Pipelines

DLT pipelines targeting the catalog are **not migrated** by this notebook. After the rename swap:

- The pipeline's configured catalog name resolves to the new (migrated) catalog.
- The pipeline will need a **full refresh** because its target tables (streaming tables, materialized views) were not moved.
- Trigger the refresh manually after migration.

### DEEP CLONE Limitations

- **Table history is not preserved.** The cloned table starts fresh; time travel queries use new version numbers.
- **DEEP CLONE is expensive** for large tables — it copies all data files, incurring S3 read/write costs.
- **Changes during migration are not captured.** DEEP CLONE is snapshot-based. Any writes to the source table during cloning will not appear in the clone.

### Function Definitions

- `information_schema.routines.routine_definition` returns `NULL` for functions the caller does not own. The notebook runner should be metastore admin or the owner of all functions.
- Java/Scala JAR-based UDFs may not be fully reconstructable from metadata alone.

### Foreign Key Constraints

FK constraints that reference tables **within the same catalog** are rewritten to point to the `_aux` catalog during migration and should work correctly after the rename swap. FK constraints referencing tables in **other catalogs** are preserved as-is.

## Migration Log Table

All operations are logged to a persistent Delta table at `<log_catalog>.default.uc_migration_log`. The table is created automatically on first run.

### Schema

| Column | Type | Description |
|---|---|---|
| `batch_id` | STRING | Timestamp-based run identifier (e.g., `20260413_143022`) |
| `catalog_name` | STRING | Source catalog being migrated |
| `phase` | STRING | Migration phase (e.g., `tables`, `views`, `rename_swap`, `validation`) |
| `object_type` | STRING | Object type (e.g., `TABLE`, `VIEW`, `SCHEMA`, `GRANT`) |
| `object_name` | STRING | Fully qualified object name |
| `status` | STRING | `SUCCESS`, `FAILED`, `SKIPPED`, `WARNING`, `PASS`, `FAIL`, `ERROR`, `IN_SCOPE`, `OUT_OF_SCOPE` |
| `message` | STRING | Details — error message, grant counts, validation results |
| `duration_seconds` | DOUBLE | How long the operation took |
| `logged_at` | TIMESTAMP | When the log entry was written (UTC) |

### Useful Queries

```sql
-- Full log for a specific migration run
SELECT * FROM <log_catalog>.default.uc_migration_log
WHERE batch_id = '20260413_143022'
ORDER BY logged_at;

-- All failures for a specific catalog across all runs
SELECT batch_id, phase, object_name, message
FROM <log_catalog>.default.uc_migration_log
WHERE catalog_name = 'prod_analytics' AND status IN ('FAILED', 'ERROR')
ORDER BY batch_id DESC, logged_at;

-- Summary by phase and status for a run
SELECT phase, status, COUNT(*) as cnt
FROM <log_catalog>.default.uc_migration_log
WHERE batch_id = '20260413_143022'
GROUP BY phase, status
ORDER BY phase, status;

-- Full inventory of what was discovered in the source catalog
SELECT object_type, object_name, status, message
FROM <log_catalog>.default.uc_migration_log
WHERE batch_id = '20260413_143022' AND phase = 'inventory'
ORDER BY object_type, object_name;

-- All out-of-scope objects (not migrated)
SELECT object_type, object_name, message
FROM <log_catalog>.default.uc_migration_log
WHERE batch_id = '20260413_143022' AND phase = 'inventory' AND status = 'OUT_OF_SCOPE'
ORDER BY object_type, object_name;

-- All warnings for a run
SELECT object_name, message
FROM <log_catalog>.default.uc_migration_log
WHERE batch_id = '20260413_143022' AND status = 'WARNING'
ORDER BY logged_at;

-- Compare two migration runs
SELECT batch_id, phase, status, COUNT(*) as cnt
FROM <log_catalog>.default.uc_migration_log
WHERE batch_id IN ('20260413_143022', '20260414_091500')
GROUP BY batch_id, phase, status
ORDER BY batch_id, phase, status;

-- Row count validation results for a run
SELECT object_name, status, message
FROM <log_catalog>.default.uc_migration_log
WHERE batch_id = '20260413_143022' AND phase = 'validation' AND object_type = 'ROW_COUNT'
ORDER BY object_name;
```

### Batch ID

Each notebook run generates a unique `batch_id` based on the UTC timestamp at execution time (format: `YYYYMMDD_HHMMSS`). This allows you to:
- Distinguish between multiple runs of the same migration
- Track retry attempts after failures
- Compare results across different migration batches

## Post-Migration Cleanup

Once you have fully validated the migration:

```sql
-- Drop the backup catalog (IRREVERSIBLE)
DROP CATALOG `<catalog>_bkp` CASCADE;
```

Only do this after thorough validation and stakeholder sign-off.

## Troubleshooting

| Issue | Cause | Resolution |
|---|---|---|
| Notebook exits with `PREFLIGHT_FAILED` | One or more preflight checks failed | Read the printed failure list, resolve each item (missing grant, collision, unreachable warehouse), and re-run |
| `_aux` catalog already exists | Previous failed run | `DROP CATALOG <catalog>_aux CASCADE` and re-run |
| `_bkp` catalog already exists | Previous migration | Drop it if no longer needed, or rename it |
| View creation fails after swap | Dependency on unmigrated object (external table, streaming table) | Manually recreate the view, adjusting references |
| Grant application fails | Principal doesn't exist or privilege not supported | Check the WARN messages in output; apply manually |
| Function migration skipped | Cannot read definition (not owner) | Run as metastore admin |
| Row count mismatch | Writes during migration | Re-clone affected tables; run during quiesce window |
