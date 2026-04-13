# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Unity Catalog — Catalog Migration Notebook
# MAGIC
# MAGIC This notebook migrates a Unity Catalog catalog that was created **without a managed storage location**
# MAGIC to a new catalog that has a proper managed storage location configured.
# MAGIC
# MAGIC ## Migration Flow
# MAGIC 1. **Inventory** — Discover all objects in the source catalog and produce a dry-run report
# MAGIC 2. **Create target catalog** (`<catalog>_aux`) with the specified managed storage location
# MAGIC 3. **Migrate schemas** (including empty ones) — properties, comments, grants, ownership
# MAGIC 4. **Migrate managed tables** via `DEEP CLONE` — then restore metadata, grants, ownership
# MAGIC 5. **Migrate functions** — recreate from definitions, restore grants, ownership
# MAGIC 6. **Migrate managed volumes** — recreate and copy files, restore grants, ownership
# MAGIC 7. **Rename swap** — `<catalog>` → `<catalog>_bkp`, `<catalog>_aux` → `<catalog>`
# MAGIC 8. **Recreate views & materialized views** — using original DDL (references resolve to new catalog post-swap)
# MAGIC 9. **Validation** — automated comparison of object counts, row counts, schemas, grants
# MAGIC
# MAGIC All operations are logged to a persistent Delta table for auditability.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Source Catalog Name")
dbutils.widgets.text("managed_storage_location", "", "Managed Storage Location (s3://...)")
dbutils.widgets.text("log_catalog", "", "Catalog for Migration Log Table")
dbutils.widgets.text("sql_warehouse_id", "", "SQL Warehouse ID (required for MV recreation)")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry Run Mode")

catalog_name = dbutils.widgets.get("catalog_name").strip()
managed_storage_location = dbutils.widgets.get("managed_storage_location").strip()
log_catalog = dbutils.widgets.get("log_catalog").strip()
sql_warehouse_id = dbutils.widgets.get("sql_warehouse_id").strip()
dry_run = dbutils.widgets.get("dry_run").strip().lower() == "true"

aux_catalog = f"{catalog_name}_aux"
bkp_catalog = f"{catalog_name}_bkp"

assert catalog_name, "catalog_name is required"
assert managed_storage_location, "managed_storage_location is required"
assert log_catalog, "log_catalog is required"

print(f"Source catalog:           {catalog_name}")
print(f"Aux catalog:              {aux_catalog}")
print(f"Backup catalog:           {bkp_catalog}")
print(f"Managed storage location: {managed_storage_location}")
print(f"Log catalog:              {log_catalog}")
print(f"SQL warehouse ID:         {sql_warehouse_id or '(not set — MVs will be skipped)'}")
print(f"Dry run:                  {dry_run}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports and Helpers

# COMMAND ----------

import time
from datetime import datetime, timezone
from collections import defaultdict
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from databricks.sdk import WorkspaceClient

# ---------------------------------------------------------------------------
# Batch ID — timestamp-based, unique per run
# ---------------------------------------------------------------------------
batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
print(f"Batch ID: {batch_id}")

# ---------------------------------------------------------------------------
# Log table setup
# ---------------------------------------------------------------------------
LOG_TABLE = f"{log_catalog}.default.uc_migration_log"

LOG_SCHEMA = StructType([
    StructField("batch_id", StringType(), False),
    StructField("catalog_name", StringType(), False),
    StructField("phase", StringType(), False),
    StructField("object_type", StringType(), False),
    StructField("object_name", StringType(), False),
    StructField("status", StringType(), False),
    StructField("message", StringType(), True),
    StructField("duration_seconds", DoubleType(), True),
    StructField("logged_at", TimestampType(), False),
])

# Create log table if it doesn't exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
        batch_id            STRING      NOT NULL,
        catalog_name        STRING      NOT NULL,
        phase               STRING      NOT NULL,
        object_type         STRING      NOT NULL,
        object_name         STRING      NOT NULL,
        status              STRING      NOT NULL,
        message             STRING,
        duration_seconds    DOUBLE,
        logged_at           TIMESTAMP   NOT NULL
    )
    COMMENT 'Migration log for Unity Catalog catalog storage migration'
""")
print(f"Log table: {LOG_TABLE}")

# ---------------------------------------------------------------------------
# Quoting / naming helpers
# ---------------------------------------------------------------------------

def q(*parts):
    """Return a backtick-quoted, dot-separated identifier."""
    return ".".join(f"`{p}`" for p in parts)


def fqn(catalog, schema, obj):
    """Fully-qualified name (unquoted) for display / keys."""
    return f"{catalog}.{schema}.{obj}"


def escape_sql(s):
    """Escape single quotes for SQL string literals."""
    if s is None:
        return None
    return s.replace("'", "\\'")

# ---------------------------------------------------------------------------
# Internal table detection (MV/DLT pipeline backing tables)
# ---------------------------------------------------------------------------

import re as _re

_INTERNAL_TABLE_PATTERNS = [
    _re.compile(r"^__materialization_"),   # MV backing tables
    _re.compile(r"^event_log_[0-9a-f]"),   # MV/DLT event log tables
]

def is_internal_table(table_name):
    """Return True if the table is an internal MV/DLT backing table that should be skipped."""
    for pattern in _INTERNAL_TABLE_PATTERNS:
        if pattern.match(table_name):
            return True
    return False

# ---------------------------------------------------------------------------
# SQL warehouse execution (for MV operations)
# ---------------------------------------------------------------------------

def run_on_warehouse(sql_statement, warehouse_id):
    """Execute SQL on a SQL warehouse via the Databricks SDK. Returns (True, result_data) or (False, error_str)."""
    from databricks.sdk.service.sql import StatementState
    try:
        w_client = WorkspaceClient()
        response = w_client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql_statement,
            wait_timeout="50s",
        )
        # If the statement is still running, poll until done
        while response.status and response.status.state in (
            StatementState.PENDING, StatementState.RUNNING
        ):
            time.sleep(5)
            response = w_client.statement_execution.get_statement(response.statement_id)

        if response.status and response.status.state == StatementState.SUCCEEDED:
            # Return result as a dict matching the REST API shape
            data_array = []
            if response.result and response.result.data_array:
                data_array = response.result.data_array
            return True, {"result": {"data_array": data_array}}
        else:
            err_msg = ""
            if response.status and response.status.error:
                err_msg = response.status.error.message or str(response.status.state)
            else:
                err_msg = str(response.status.state) if response.status else "Unknown error"
            return False, err_msg
    except Exception as e:
        return False, str(e)

# ---------------------------------------------------------------------------
# Log writing helper
# ---------------------------------------------------------------------------

def write_log(phase, object_type, object_name, status, message=None, duration_seconds=None):
    """Append a row to the migration log table."""
    row = [(
        batch_id,
        catalog_name,
        phase,
        object_type,
        object_name,
        status,
        message,
        duration_seconds,
        datetime.now(timezone.utc),
    )]
    df = spark.createDataFrame(row, schema=LOG_SCHEMA)
    df.write.mode("append").saveAsTable(LOG_TABLE)

# ---------------------------------------------------------------------------
# SQL execution helpers
# ---------------------------------------------------------------------------

def run(sql):
    """Execute SQL and return the DataFrame."""
    return spark.sql(sql)


def run_quiet(sql):
    """Execute SQL, return (True, df) on success or (False, error_str) on failure."""
    try:
        df = spark.sql(sql)
        return True, df
    except Exception as e:
        return False, str(e)

# ---------------------------------------------------------------------------
# Metadata retrieval helpers
# ---------------------------------------------------------------------------

def get_catalog_info(cat):
    """Return dict with owner and comment for a catalog."""
    rows = run(f"DESCRIBE CATALOG EXTENDED {q(cat)}").collect()
    info = {r[0]: r[1] for r in rows}
    return {
        "owner": info.get("Owner", info.get("owner", "")),
        "comment": info.get("Comment", info.get("comment", "")),
    }


def get_schema_info(cat, schema):
    """Return dict with owner, comment, and properties for a schema."""
    rows = run(f"DESCRIBE SCHEMA EXTENDED {q(cat, schema)}").collect()
    info = {}
    for r in rows:
        key = r[0].strip() if r[0] else ""
        val = r[1].strip() if r[1] else ""
        if key.lower() == "owner":
            info["owner"] = val
        elif key.lower() == "comment":
            info["comment"] = val
        elif key.lower() == "properties":
            info["properties"] = val
    return {
        "owner": info.get("owner", ""),
        "comment": info.get("comment", ""),
        "properties": info.get("properties", ""),
    }


def get_grants(object_type, full_name_quoted):
    """Return list of (principal, action_type) tuples for direct grants."""
    ok, df = run_quiet(f"SHOW GRANTS ON {object_type} {full_name_quoted}")
    if not ok:
        return []
    rows = df.collect()
    results = []
    for r in rows:
        principal = r[0]
        action_type = r[1]
        results.append((principal, action_type))
    return results


def apply_grants(grants, object_type, target_name_quoted, phase, object_name):
    """Apply a list of (principal, action_type) grants to the target object."""
    success, failed = 0, 0
    for principal, action_type in grants:
        if action_type.upper() in ("OWN", "OWNERSHIP"):
            continue
        ok, err = run_quiet(
            f"GRANT {action_type} ON {object_type} {target_name_quoted} TO `{principal}`"
        )
        if ok:
            success += 1
        else:
            print(f"    WARN grant {action_type} to {principal}: {err}")
            write_log(phase, "GRANT", f"{object_name} -> {principal}:{action_type}", "FAILED", str(err))
            failed += 1
    return success, failed


def set_owner(object_type, full_name_quoted, owner):
    """Set owner on an object. Returns True on success."""
    if not owner:
        return True
    ok, err = run_quiet(
        f"ALTER {object_type} {full_name_quoted} SET OWNER TO `{owner}`"
    )
    if not ok:
        print(f"    WARN set owner {owner}: {err}")
    return ok


def get_table_comment(cat, schema, table):
    """Return the table/view comment (description), or empty string."""
    rows = run(
        f"SELECT comment FROM {q(cat)}.information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
    ).collect()
    return rows[0][0] if rows and rows[0][0] else ""


def get_table_owner(cat, schema, table):
    """Return the table/view owner."""
    rows = run(
        f"SELECT table_owner FROM {q(cat)}.information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
    ).collect()
    return rows[0][0] if rows and rows[0][0] else ""


def get_table_tags(cat, schema, table):
    """Return list of (tag_name, tag_value) for a table."""
    ok, df = run_quiet(
        f"SELECT tag_name, tag_value FROM {q(cat)}.information_schema.table_tags "
        f"WHERE schema_name = '{schema}' AND table_name = '{table}'"
    )
    if not ok:
        return []
    return [(r[0], r[1]) for r in df.collect()]


def get_column_tags(cat, schema, table):
    """Return list of (column_name, tag_name, tag_value) for columns of a table."""
    ok, df = run_quiet(
        f"SELECT column_name, tag_name, tag_value "
        f"FROM {q(cat)}.information_schema.column_tags "
        f"WHERE schema_name = '{schema}' AND table_name = '{table}'"
    )
    if not ok:
        return []
    return [(r[0], r[1], r[2]) for r in df.collect()]


def get_pk_constraints(cat, schema, table):
    """Return list of PK constraints as (constraint_name, [columns])."""
    ok, df = run_quiet(f"""
        SELECT tc.constraint_name, kcu.column_name, kcu.ordinal_position
        FROM {q(cat)}.information_schema.table_constraints tc
        JOIN {q(cat)}.information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
          AND tc.table_name = kcu.table_name
        WHERE tc.table_schema = '{schema}'
          AND tc.table_name = '{table}'
          AND tc.constraint_type = 'PRIMARY KEY'
        ORDER BY kcu.ordinal_position
    """)
    if not ok:
        return []
    constraints = defaultdict(list)
    for r in df.collect():
        constraints[r[0]].append(r[1])
    return list(constraints.items())


def get_fk_constraints(cat, schema, table):
    """Return FK constraints as list of (constraint_name, [columns], ref_table_fqn, [ref_columns])."""
    ok, df = run_quiet(f"""
        SELECT
            tc.constraint_name,
            kcu.column_name,
            kcu.ordinal_position,
            rc.unique_constraint_catalog,
            rc.unique_constraint_schema,
            rc.unique_constraint_name
        FROM {q(cat)}.information_schema.table_constraints tc
        JOIN {q(cat)}.information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
          AND tc.table_name = kcu.table_name
        JOIN {q(cat)}.information_schema.referential_constraints rc
          ON tc.constraint_name = rc.constraint_name
        WHERE tc.table_schema = '{schema}'
          AND tc.table_name = '{table}'
          AND tc.constraint_type = 'FOREIGN KEY'
        ORDER BY kcu.ordinal_position
    """)
    if not ok:
        return []
    fk_map = defaultdict(lambda: {"cols": [], "ref_cat": "", "ref_schema": "", "ref_constraint": ""})
    for r in df.collect():
        entry = fk_map[r[0]]
        entry["cols"].append(r[1])
        entry["ref_cat"] = r[3]
        entry["ref_schema"] = r[4]
        entry["ref_constraint"] = r[5]
    results = []
    for cname, info in fk_map.items():
        ok2, df2 = run_quiet(f"""
            SELECT kcu.table_name, kcu.column_name
            FROM {q(info['ref_cat'])}.information_schema.key_column_usage kcu
            WHERE kcu.constraint_name = '{info['ref_constraint']}'
              AND kcu.table_schema = '{info['ref_schema']}'
            ORDER BY kcu.ordinal_position
        """)
        if ok2:
            ref_rows = df2.collect()
            ref_table = ref_rows[0][0] if ref_rows else ""
            ref_cols = [r[1] for r in ref_rows]
            ref_fqn = fqn(info["ref_cat"], info["ref_schema"], ref_table)
            results.append((cname, info["cols"], ref_fqn, ref_cols))
    return results


def get_ddl(cat, schema, obj):
    """Get the full DDL for a table, view, or materialized view via SHOW CREATE TABLE."""
    ok, df = run_quiet(f"SHOW CREATE TABLE {q(cat, schema, obj)}")
    if not ok:
        return None
    return df.first()[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 1 — Inventory & Dry-Run Report

# COMMAND ----------

print(f"Inventorying catalog: {catalog_name}")
print("=" * 60)

# ---- Schemas ----
schemas_df = run(f"""
    SELECT schema_name
    FROM {q(catalog_name)}.information_schema.schemata
    WHERE schema_name != 'information_schema'
    ORDER BY schema_name
""")
schemas = [r[0] for r in schemas_df.collect()]
print(f"\nSchemas ({len(schemas)}): {schemas}")

# ---- Tables by type ----
tables_df = run(f"""
    SELECT table_schema, table_name, table_type, table_owner, comment
    FROM {q(catalog_name)}.information_schema.tables
    WHERE table_schema != 'information_schema'
    ORDER BY table_schema, table_name
""")
all_tables = tables_df.collect()

managed_tables_all = [r for r in all_tables if r.table_type in ("MANAGED", "TABLE", "BASE TABLE")]
internal_tables = [r for r in managed_tables_all if is_internal_table(r.table_name)]
managed_tables = [r for r in managed_tables_all if not is_internal_table(r.table_name)]
external_tables = [r for r in all_tables if r.table_type == "EXTERNAL"]
views = [r for r in all_tables if r.table_type == "VIEW"]
materialized_views = [r for r in all_tables if r.table_type in ("MATERIALIZED_VIEW",)]
streaming_tables = [r for r in all_tables if r.table_type in ("STREAMING_TABLE",)]
unknown_types = [r for r in all_tables if r.table_type not in (
    "MANAGED", "TABLE", "BASE TABLE", "EXTERNAL", "VIEW", "MATERIALIZED_VIEW", "STREAMING_TABLE"
)]

print(f"\nManaged tables:      {len(managed_tables)}")
if internal_tables:
    print(f"Internal tables:     {len(internal_tables)}  (SKIPPED — MV/DLT backing tables)")
print(f"Views:               {len(views)}")
print(f"Materialized views:  {len(materialized_views)}")
print(f"External tables:     {len(external_tables)}  (OUT OF SCOPE)")
print(f"Streaming tables:    {len(streaming_tables)}  (OUT OF SCOPE)")
if unknown_types:
    print(f"Unknown types:       {len(unknown_types)}")
    for r in unknown_types:
        print(f"  - {fqn(catalog_name, r.table_schema, r.table_name)} (type={r.table_type})")

# ---- Volumes ----
ok_vol, volumes_df = run_quiet(f"""
    SELECT volume_schema, volume_name, volume_type, comment
    FROM {q(catalog_name)}.information_schema.volumes
    WHERE volume_schema != 'information_schema'
    ORDER BY volume_schema, volume_name
""")
volumes = volumes_df.collect() if ok_vol else []
managed_volumes = [v for v in volumes if v.volume_type == "MANAGED"]
external_volumes = [v for v in volumes if v.volume_type != "MANAGED"]
print(f"\nManaged volumes:     {len(managed_volumes)}")
if external_volumes:
    print(f"External volumes:    {len(external_volumes)}  (OUT OF SCOPE)")

# ---- Functions ----
ok_fn, functions_df = run_quiet(f"""
    SELECT routine_schema, routine_name, routine_type, routine_body,
           data_type, full_data_type, comment
    FROM {q(catalog_name)}.information_schema.routines
    WHERE routine_schema != 'information_schema'
    ORDER BY routine_schema, routine_name
""")
functions = functions_df.collect() if ok_fn else []
print(f"Functions:           {len(functions)}")

# ---- Log inventory to table ----
# Summary row
write_log("inventory", "CATALOG", catalog_name, "SUCCESS",
          f"schemas={len(schemas)} tables={len(managed_tables)} views={len(views)} "
          f"mvs={len(materialized_views)} functions={len(functions)} volumes={len(managed_volumes)}")

# Individual schemas
for s in schemas:
    write_log("inventory", "SCHEMA", fqn(catalog_name, s, ""), "IN_SCOPE")

# Individual tables/views/MVs — in-scope and out-of-scope
for r in managed_tables:
    write_log("inventory", "TABLE", fqn(catalog_name, r.table_schema, r.table_name),
              "IN_SCOPE", f"type={r.table_type} owner={r.table_owner}")
for r in views:
    write_log("inventory", "VIEW", fqn(catalog_name, r.table_schema, r.table_name),
              "IN_SCOPE", f"owner={r.table_owner}")
for r in materialized_views:
    write_log("inventory", "MATERIALIZED_VIEW", fqn(catalog_name, r.table_schema, r.table_name),
              "IN_SCOPE", f"owner={r.table_owner}")
for r in external_tables:
    write_log("inventory", "TABLE", fqn(catalog_name, r.table_schema, r.table_name),
              "OUT_OF_SCOPE", f"type=EXTERNAL owner={r.table_owner}")
for r in streaming_tables:
    write_log("inventory", "TABLE", fqn(catalog_name, r.table_schema, r.table_name),
              "OUT_OF_SCOPE", f"type=STREAMING_TABLE owner={r.table_owner}")
for r in unknown_types:
    write_log("inventory", "TABLE", fqn(catalog_name, r.table_schema, r.table_name),
              "OUT_OF_SCOPE", f"type={r.table_type} (unknown)")
for r in internal_tables:
    write_log("inventory", "TABLE", fqn(catalog_name, r.table_schema, r.table_name),
              "SKIPPED", f"Internal MV/DLT backing table")

# Individual functions
for r in functions:
    write_log("inventory", "FUNCTION", fqn(catalog_name, r.routine_schema, r.routine_name),
              "IN_SCOPE", f"body={r.routine_body}")

# Individual volumes
for v in managed_volumes:
    write_log("inventory", "VOLUME", fqn(catalog_name, v.volume_schema, v.volume_name),
              "IN_SCOPE", f"type=MANAGED")
for v in external_volumes:
    write_log("inventory", "VOLUME", fqn(catalog_name, v.volume_schema, v.volume_name),
              "OUT_OF_SCOPE", f"type={v.volume_type}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Warnings & Risk Detection

# COMMAND ----------

warnings = []

if external_tables:
    warnings.append(
        f"EXTERNAL TABLES ({len(external_tables)}): Not migrated. "
        "These tables reference external storage and are out of scope."
    )
if streaming_tables:
    warnings.append(
        f"STREAMING TABLES ({len(streaming_tables)}): Not migrated. "
        "Streaming tables cannot be cloned and require full re-ingestion from source."
    )
if external_volumes:
    warnings.append(
        f"EXTERNAL VOLUMES ({len(external_volumes)}): Not migrated. "
        "External volumes reference external storage and are out of scope."
    )

# -- Check for Delta Sharing --
try:
    shares = run("SHOW SHARES").collect()
    affected_shares = []
    for share in shares:
        share_name = share[0]
        ok_s, share_df = run_quiet(f"SHOW ALL IN SHARE `{share_name}`")
        if ok_s:
            for obj in share_df.collect():
                obj_name = str(obj[0]) if obj[0] else ""
                if obj_name.lower().startswith(f"{catalog_name.lower()}."):
                    affected_shares.append(share_name)
                    break
    if affected_shares:
        warnings.append(
            f"DELTA SHARING: {len(affected_shares)} share(s) reference objects in this catalog: "
            f"{affected_shares}. After migration, share references will point to objects in "
            f"'{bkp_catalog}'. You must manually update shares to reference the migrated catalog. "
            "See readme.md for details."
        )
except Exception:
    warnings.append(
        "DELTA SHARING: Could not check for shares (insufficient permissions or feature not enabled). "
        "Manually verify whether any shares reference this catalog before proceeding."
    )

# -- Check for DLT pipelines --
warnings.append(
    "DLT PIPELINES: This notebook does not detect or migrate DLT/Lakeflow pipelines. "
    "If any pipelines target this catalog, they will need a full refresh after migration. "
    "Check the Workflows UI or use the Pipelines API to identify affected pipelines."
)

# -- Check for aux/bkp catalog name collisions --
try:
    existing = [r[0] for r in run("SHOW CATALOGS").collect()]
    if aux_catalog in existing:
        warnings.append(
            f"BLOCKING: Catalog '{aux_catalog}' already exists. "
            "Drop it or choose a different source catalog before proceeding."
        )
    if bkp_catalog in existing:
        warnings.append(
            f"BLOCKING: Catalog '{bkp_catalog}' already exists. "
            "Drop it or choose a different source catalog before proceeding."
        )
except Exception as e:
    warnings.append(f"Could not check existing catalogs: {e}")

# -- Materialized views warning --
if materialized_views:
    warnings.append(
        f"MATERIALIZED VIEWS ({len(materialized_views)}): Will be recreated after the rename swap. "
        "This triggers a full recomputation which may take significant time and compute."
    )

# -- Log warnings --
for w in warnings:
    write_log("inventory", "WARNING", catalog_name, "WARNING", w)

# -- Print warnings --
print("=" * 60)
print("WARNINGS & RISKS")
print("=" * 60)
if warnings:
    for i, w in enumerate(warnings, 1):
        print(f"\n  [{i}] {w}")
else:
    print("\n  No warnings detected.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dry-Run Summary

# COMMAND ----------

print("=" * 60)
print("DRY-RUN SUMMARY")
print("=" * 60)
print(f"""
Source catalog:            {catalog_name}
Target catalog (aux):      {aux_catalog}
Backup catalog:            {bkp_catalog}
Managed storage location:  {managed_storage_location}
Batch ID:                  {batch_id}

Objects to migrate:
  Schemas:                 {len(schemas)}
  Managed tables:          {len(managed_tables)}
  Views:                   {len(views)}
  Materialized views:      {len(materialized_views)}
  Functions:               {len(functions)}
  Managed volumes:         {len(managed_volumes)}

Out of scope (will NOT be migrated):
  External tables:         {len(external_tables)}
  Streaming tables:        {len(streaming_tables)}
  External volumes:        {len(external_volumes)}
""")

if dry_run:
    write_log("dry_run", "CATALOG", catalog_name, "SUCCESS", "Dry run completed")
    print("*** DRY RUN MODE — no changes will be made. ***")
    print("Set dry_run = false to execute the migration.")
    dbutils.notebook.exit("DRY_RUN_COMPLETE")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Phase 2 — Create Target Catalog & Migrate Schemas

# COMMAND ----------

# ---------------------------------------------------------------------------
# Create the aux catalog
# ---------------------------------------------------------------------------
print(f"Creating catalog {aux_catalog} with managed storage at {managed_storage_location} ...")
source_info = get_catalog_info(catalog_name)

t0 = time.time()
create_sql = f"CREATE CATALOG {q(aux_catalog)} MANAGED LOCATION '{managed_storage_location}'"
if source_info["comment"]:
    create_sql += f" COMMENT '{escape_sql(source_info['comment'])}'"
run(create_sql)
elapsed = time.time() - t0
print(f"  Catalog {aux_catalog} created.")
write_log("create_catalog", "CATALOG", aux_catalog, "SUCCESS", None, elapsed)

# Apply catalog-level grants
cat_grants = get_grants("CATALOG", q(catalog_name))
gs, gf = apply_grants(cat_grants, "CATALOG", q(aux_catalog), "create_catalog", aux_catalog)
print(f"  Catalog grants applied: {gs} succeeded, {gf} failed.")

# ---------------------------------------------------------------------------
# Migrate schemas
# ---------------------------------------------------------------------------
print(f"\nMigrating {len(schemas)} schema(s) ...")

for schema_name in schemas:
    print(f"\n  Schema: {catalog_name}.{schema_name}")
    t0 = time.time()
    try:
        schema_info = get_schema_info(catalog_name, schema_name)

        if schema_name.lower() == "default":
            if schema_info["comment"]:
                run(f"COMMENT ON SCHEMA {q(aux_catalog, schema_name)} IS '{escape_sql(schema_info['comment'])}'")
        else:
            create_schema_sql = f"CREATE SCHEMA {q(aux_catalog, schema_name)}"
            if schema_info["comment"]:
                create_schema_sql += f" COMMENT '{escape_sql(schema_info['comment'])}'"
            run(create_schema_sql)

        if schema_info["properties"] and schema_info["properties"] != "(())" and schema_info["properties"] != "":
            try:
                run(f"ALTER SCHEMA {q(aux_catalog, schema_name)} SET DBPROPERTIES {schema_info['properties']}")
            except Exception as e:
                print(f"    WARN properties: {e}")

        s_grants = get_grants("SCHEMA", q(catalog_name, schema_name))
        gs, gf = apply_grants(s_grants, "SCHEMA", q(aux_catalog, schema_name), "schemas", fqn(aux_catalog, schema_name, ""))

        set_owner("SCHEMA", q(aux_catalog, schema_name), schema_info["owner"])

        elapsed = time.time() - t0
        write_log("schemas", "SCHEMA", fqn(catalog_name, schema_name, ""), "SUCCESS",
                  f"grants={gs} owner={schema_info['owner']}", elapsed)
        print(f"    OK (grants: {gs}, owner: {schema_info['owner']}) [{elapsed:.1f}s]")

    except Exception as e:
        elapsed = time.time() - t0
        write_log("schemas", "SCHEMA", fqn(catalog_name, schema_name, ""), "FAILED", str(e), elapsed)
        print(f"    FAILED: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 3 — Migrate Managed Tables (DEEP CLONE)

# COMMAND ----------

print(f"Migrating {len(managed_tables)} managed table(s) via DEEP CLONE ...")

for tbl in managed_tables:
    schema, table = tbl.table_schema, tbl.table_name
    src = q(catalog_name, schema, table)
    tgt = q(aux_catalog, schema, table)
    obj_fqn = fqn(catalog_name, schema, table)
    print(f"\n  Table: {obj_fqn}")
    t0 = time.time()

    try:
        # Deep clone
        run(f"CREATE TABLE {tgt} DEEP CLONE {src}")
        clone_elapsed = time.time() - t0
        print(f"    DEEP CLONE complete ({clone_elapsed:.1f}s)")

        # Restore table comment/description
        comment = get_table_comment(catalog_name, schema, table)
        if comment:
            run(f"COMMENT ON TABLE {tgt} IS '{escape_sql(comment)}'")

        # Restore table tags
        tags = get_table_tags(catalog_name, schema, table)
        if tags:
            tag_pairs = ", ".join(f"'{t[0]}' = '{escape_sql(t[1])}'" for t in tags)
            run(f"ALTER TABLE {tgt} SET TAGS ({tag_pairs})")
            print(f"    Tags restored: {len(tags)}")

        # Restore column tags
        col_tags = get_column_tags(catalog_name, schema, table)
        if col_tags:
            by_col = defaultdict(list)
            for ct in col_tags:
                by_col[ct[0]].append((ct[1], ct[2]))
            for col_name, ctags in by_col.items():
                tag_pairs = ", ".join(f"'{t[0]}' = '{escape_sql(t[1])}'" for t in ctags)
                run(f"ALTER TABLE {tgt} ALTER COLUMN `{col_name}` SET TAGS ({tag_pairs})")
            print(f"    Column tags restored: {len(col_tags)}")

        # Restore PK constraints
        pks = get_pk_constraints(catalog_name, schema, table)
        for pk_name, pk_cols in pks:
            cols_str = ", ".join(f"`{c}`" for c in pk_cols)
            ok, err = run_quiet(
                f"ALTER TABLE {tgt} ADD CONSTRAINT `{pk_name}` PRIMARY KEY ({cols_str})"
            )
            if ok:
                print(f"    PK constraint restored: {pk_name}")
            else:
                print(f"    WARN PK constraint {pk_name}: {err}")

        # Restore FK constraints
        fks = get_fk_constraints(catalog_name, schema, table)
        for fk_name, fk_cols, ref_fqn_str, ref_cols in fks:
            cols_str = ", ".join(f"`{c}`" for c in fk_cols)
            ref_cols_str = ", ".join(f"`{c}`" for c in ref_cols)
            if ref_fqn_str.startswith(f"{catalog_name}."):
                ref_fqn_str = ref_fqn_str.replace(f"{catalog_name}.", f"{aux_catalog}.", 1)
            ref_parts = ref_fqn_str.split(".")
            ref_quoted = q(*ref_parts)
            ok, err = run_quiet(
                f"ALTER TABLE {tgt} ADD CONSTRAINT `{fk_name}` "
                f"FOREIGN KEY ({cols_str}) REFERENCES {ref_quoted} ({ref_cols_str})"
            )
            if ok:
                print(f"    FK constraint restored: {fk_name}")
            else:
                print(f"    WARN FK constraint {fk_name}: {err}")

        # Apply grants
        t_grants = get_grants("TABLE", src)
        gs, gf = apply_grants(t_grants, "TABLE", tgt, "tables", obj_fqn)

        # Transfer ownership
        owner = get_table_owner(catalog_name, schema, table)
        set_owner("TABLE", tgt, owner)

        elapsed = time.time() - t0
        write_log("tables", "TABLE", obj_fqn, "SUCCESS",
                  f"clone={clone_elapsed:.1f}s grants={gs} owner={owner}", elapsed)
        print(f"    OK (grants: {gs}, owner: {owner}) [{elapsed:.1f}s total]")

    except Exception as e:
        elapsed = time.time() - t0
        write_log("tables", "TABLE", obj_fqn, "FAILED", str(e), elapsed)
        print(f"    FAILED: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 4 — Migrate Functions

# COMMAND ----------

print(f"Migrating {len(functions)} function(s) ...")

for fn_row in functions:
    schema = fn_row.routine_schema
    func_name = fn_row.routine_name
    src_fq = q(catalog_name, schema, func_name)
    tgt_fq = q(aux_catalog, schema, func_name)
    obj_fqn = fqn(catalog_name, schema, func_name)
    print(f"\n  Function: {obj_fqn}")
    t0 = time.time()

    try:
        routine = run(f"""
            SELECT routine_definition, routine_body, full_data_type,
                   is_deterministic, sql_data_access, comment, external_language
            FROM {q(catalog_name)}.information_schema.routines
            WHERE routine_schema = '{schema}' AND routine_name = '{func_name}'
        """).first()

        if not routine or not routine.routine_definition:
            elapsed = time.time() - t0
            msg = "Could not read function definition (must be owner or admin)"
            print(f"    WARN: {msg}. Skipping.")
            write_log("functions", "FUNCTION", obj_fqn, "SKIPPED", msg, elapsed)
            continue

        ok_params, params_df = run_quiet(f"""
            SELECT parameter_name, full_data_type, ordinal_position, parameter_mode
            FROM {q(catalog_name)}.information_schema.parameters
            WHERE specific_schema = '{schema}' AND specific_name = '{func_name}'
            ORDER BY ordinal_position
        """)
        params = params_df.collect() if ok_params else []
        input_params = [p for p in params if p.parameter_mode == "IN"]
        param_list = ", ".join(f"`{p.parameter_name}` {p.full_data_type}" for p in input_params)

        create_ddl = f"CREATE FUNCTION {tgt_fq}({param_list})"
        create_ddl += f"\nRETURNS {routine.full_data_type}"

        if routine.comment:
            create_ddl += f"\nCOMMENT '{escape_sql(routine.comment)}'"
        if routine.is_deterministic and routine.is_deterministic.upper() == "YES":
            create_ddl += "\nDETERMINISTIC"
        # Note: sql_data_access (e.g., CONTAINS_SQL, NO_SQL) is not valid in CREATE FUNCTION syntax

        body = routine.routine_definition
        lang = (routine.external_language or "").upper()
        r_body = (routine.routine_body or "").upper()

        if r_body == "SQL":
            create_ddl += f"\nRETURN {body}"
        elif r_body == "EXTERNAL" and lang == "PYTHON":
            create_ddl += f"\nLANGUAGE PYTHON\nAS $$\n{body}\n$$"
        else:
            print(f"    WARN: Unsupported function type ({r_body}/{lang}). Attempting SQL RETURN.")
            create_ddl += f"\nRETURN {body}"

        run(create_ddl)

        f_grants = get_grants("FUNCTION", src_fq)
        gs, gf = apply_grants(f_grants, "FUNCTION", tgt_fq, "functions", obj_fqn)

        ok_own, owner_df = run_quiet(f"""
            SELECT specific_owner FROM {q(catalog_name)}.information_schema.routines
            WHERE routine_schema = '{schema}' AND routine_name = '{func_name}'
        """)
        if ok_own:
            row = owner_df.first()
            if row and row[0]:
                set_owner("FUNCTION", tgt_fq, row[0])

        elapsed = time.time() - t0
        write_log("functions", "FUNCTION", obj_fqn, "SUCCESS", f"grants={gs}", elapsed)
        print(f"    OK (grants: {gs}) [{elapsed:.1f}s]")

    except Exception as e:
        elapsed = time.time() - t0
        write_log("functions", "FUNCTION", obj_fqn, "FAILED", str(e), elapsed)
        print(f"    FAILED: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 5 — Migrate Managed Volumes

# COMMAND ----------

print(f"Migrating {len(managed_volumes)} managed volume(s) ...")

for vol in managed_volumes:
    schema = vol.volume_schema
    vol_name = vol.volume_name
    src_path = f"/Volumes/{catalog_name}/{schema}/{vol_name}"
    tgt_path = f"/Volumes/{aux_catalog}/{schema}/{vol_name}"
    src_fq = q(catalog_name, schema, vol_name)
    tgt_fq = q(aux_catalog, schema, vol_name)
    obj_fqn = fqn(catalog_name, schema, vol_name)
    print(f"\n  Volume: {obj_fqn}")
    t0 = time.time()

    try:
        create_sql = f"CREATE VOLUME {tgt_fq}"
        vol_comment = vol.comment
        if vol_comment:
            create_sql += f" COMMENT '{escape_sql(vol_comment)}'"
        run(create_sql)

        files_copied = 0
        try:
            files = dbutils.fs.ls(src_path)
            if files:
                dbutils.fs.cp(src_path, tgt_path, recurse=True)
                files_copied = len(files)
                print(f"    Files copied ({files_copied} top-level entries)")
            else:
                print(f"    Volume is empty, no files to copy")
        except Exception as e:
            print(f"    WARN copying files: {e}")

        v_grants = get_grants("VOLUME", src_fq)
        gs, gf = apply_grants(v_grants, "VOLUME", tgt_fq, "volumes", obj_fqn)

        ok_own, own_df = run_quiet(f"""
            SELECT volume_owner FROM {q(catalog_name)}.information_schema.volumes
            WHERE volume_schema = '{schema}' AND volume_name = '{vol_name}'
        """)
        if ok_own:
            row = own_df.first()
            if row and row[0]:
                set_owner("VOLUME", tgt_fq, row[0])

        elapsed = time.time() - t0
        write_log("volumes", "VOLUME", obj_fqn, "SUCCESS",
                  f"files={files_copied} grants={gs}", elapsed)
        print(f"    OK (grants: {gs}) [{elapsed:.1f}s]")

    except Exception as e:
        elapsed = time.time() - t0
        write_log("volumes", "VOLUME", obj_fqn, "FAILED", str(e), elapsed)
        print(f"    FAILED: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 6 — Capture View & Materialized View Definitions
# MAGIC
# MAGIC We capture DDL now (before the rename swap) and recreate after the swap so that
# MAGIC catalog references in view definitions resolve to the new (migrated) catalog.

# COMMAND ----------

view_definitions = []
# Also get view definitions from information_schema as fallback
ok_vdefs, vdefs_df = run_quiet(f"""
    SELECT table_schema, table_name, view_definition
    FROM {q(catalog_name)}.information_schema.views
    WHERE table_schema != 'information_schema'
""")
view_def_map = {}
if ok_vdefs:
    for r in vdefs_df.collect():
        view_def_map[(r[0], r[1])] = r[2]

for v in views:
    schema, view_name = v.table_schema, v.table_name
    ddl = get_ddl(catalog_name, schema, view_name)
    view_body = view_def_map.get((schema, view_name), None)
    grants = get_grants("VIEW", q(catalog_name, schema, view_name))
    owner = get_table_owner(catalog_name, schema, view_name)
    comment = get_table_comment(catalog_name, schema, view_name)
    tags = get_table_tags(catalog_name, schema, view_name)
    view_definitions.append({
        "schema": schema,
        "name": view_name,
        "ddl": ddl,
        "view_body": view_body,
        "grants": grants,
        "owner": owner,
        "comment": comment,
        "tags": tags,
    })
    print(f"  Captured view DDL: {catalog_name}.{schema}.{view_name}")

mv_definitions = []
for mv in materialized_views:
    schema, mv_name = mv.table_schema, mv.table_name
    # Try to get DDL from cluster first; fall back to SQL warehouse for MVs
    ddl = get_ddl(catalog_name, schema, mv_name)
    if not ddl and sql_warehouse_id:
        ok, result = run_on_warehouse(
            f"SHOW CREATE TABLE {q(catalog_name, schema, mv_name)}", sql_warehouse_id
        )
        if ok:
            rows = result.get("result", {}).get("data_array", [])
            ddl = rows[0][0] if rows else None
    grants = get_grants("TABLE", q(catalog_name, schema, mv_name))
    owner = get_table_owner(catalog_name, schema, mv_name)
    comment = get_table_comment(catalog_name, schema, mv_name)
    tags = get_table_tags(catalog_name, schema, mv_name)
    mv_definitions.append({
        "schema": schema,
        "name": mv_name,
        "ddl": ddl,
        "grants": grants,
        "owner": owner,
        "comment": comment,
        "tags": tags,
    })
    print(f"  Captured MV DDL:   {catalog_name}.{schema}.{mv_name} ({'OK' if ddl else 'NO DDL'})")

print(f"\nCaptured {len(view_definitions)} view(s) and {len(mv_definitions)} materialized view(s).")
write_log("capture_ddl", "VIEW", catalog_name, "SUCCESS", f"views={len(view_definitions)} mvs={len(mv_definitions)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 7 — Rename Swap
# MAGIC
# MAGIC 1. Rename the original catalog → `_bkp`
# MAGIC 2. Rename the aux catalog → original name
# MAGIC
# MAGIC **After this step, the original catalog name points to the newly migrated catalog.**

# COMMAND ----------

print("Performing rename swap ...")
t0 = time.time()
w = WorkspaceClient()

print(f"  Step 1: {catalog_name} → {bkp_catalog}")
w.catalogs.update(name=catalog_name, new_name=bkp_catalog)
print(f"    Done.")

print(f"  Step 2: {aux_catalog} → {catalog_name}")
w.catalogs.update(name=aux_catalog, new_name=catalog_name)
print(f"    Done.")

elapsed = time.time() - t0
write_log("rename_swap", "CATALOG", catalog_name, "SUCCESS",
          f"{catalog_name} -> {bkp_catalog}, {aux_catalog} -> {catalog_name}", elapsed)

print(f"\nRename swap complete.")
print(f"  '{catalog_name}' now points to the migrated catalog (was {aux_catalog}).")
print(f"  '{bkp_catalog}' is the original catalog backup.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 8 — Recreate Views & Materialized Views
# MAGIC
# MAGIC Views are recreated **after** the rename swap so that references like
# MAGIC `catalog.schema.table` in the view definition resolve to the migrated catalog.

# COMMAND ----------

# ---------------------------------------------------------------------------
# Recreate regular views (with retry loop for dependency ordering)
# ---------------------------------------------------------------------------
print(f"Recreating {len(view_definitions)} view(s) ...")

remaining = list(view_definitions)
max_retries = 10

for attempt in range(1, max_retries + 1):
    if not remaining:
        break
    still_failing = []
    for vdef in remaining:
        schema, view_name = vdef["schema"], vdef["name"]
        ddl = vdef["ddl"]
        view_body = vdef.get("view_body")
        tgt_fq = q(catalog_name, schema, view_name)
        obj_fqn = fqn(catalog_name, schema, view_name)

        if not ddl and not view_body:
            print(f"  SKIP {obj_fqn}: no DDL captured")
            write_log("views", "VIEW", obj_fqn, "SKIPPED", "No DDL captured")
            continue

        t0 = time.time()
        # Try SHOW CREATE TABLE DDL first
        ok, err = False, "no DDL"
        if ddl:
            ok, err = run_quiet(ddl)
        # Fallback: build CREATE VIEW from view_body (information_schema.views)
        if not ok and view_body:
            comment_clause = ""
            if vdef["comment"]:
                comment_clause = f" COMMENT '{escape_sql(vdef['comment'])}'"
            fallback_ddl = f"CREATE VIEW {tgt_fq}{comment_clause} AS {view_body}"
            ok, err = run_quiet(fallback_ddl)
        if ok:
            if vdef["comment"]:
                run_quiet(f"COMMENT ON TABLE {tgt_fq} IS '{escape_sql(vdef['comment'])}'")

            if vdef["tags"]:
                tag_pairs = ", ".join(f"'{t[0]}' = '{escape_sql(t[1])}'" for t in vdef["tags"])
                run_quiet(f"ALTER VIEW {tgt_fq} SET TAGS ({tag_pairs})")

            gs, gf = apply_grants(vdef["grants"], "VIEW", tgt_fq, "views", obj_fqn)
            set_owner("VIEW", tgt_fq, vdef["owner"])

            elapsed = time.time() - t0
            write_log("views", "VIEW", obj_fqn, "SUCCESS",
                      f"attempt={attempt} grants={gs}", elapsed)
            print(f"  OK: {obj_fqn} (attempt {attempt}) [{elapsed:.1f}s]")
        else:
            still_failing.append(vdef)

    remaining = still_failing
    if remaining and attempt < max_retries:
        print(f"  Retry {attempt}: {len(remaining)} view(s) pending (likely dependency ordering) ...")

if remaining:
    for vdef in remaining:
        obj_fqn = fqn(catalog_name, vdef["schema"], vdef["name"])
        write_log("views", "VIEW", obj_fqn, "FAILED", f"Failed after {max_retries} retries")
        print(f"  FAILED after {max_retries} retries: {obj_fqn}")

# ---------------------------------------------------------------------------
# Recreate materialized views (requires SQL warehouse)
# ---------------------------------------------------------------------------
print(f"\nRecreating {len(mv_definitions)} materialized view(s) ...")

if mv_definitions and not sql_warehouse_id:
    print("  WARNING: sql_warehouse_id not set — MV recreation requires a SQL warehouse.")
    print("  All materialized views will be SKIPPED. Re-run with sql_warehouse_id to migrate MVs.")
    for mvdef in mv_definitions:
        obj_fqn = fqn(catalog_name, mvdef["schema"], mvdef["name"])
        write_log("materialized_views", "MATERIALIZED_VIEW", obj_fqn, "SKIPPED",
                  "sql_warehouse_id not provided")

elif mv_definitions and sql_warehouse_id:
    for mvdef in mv_definitions:
        schema, mv_name, ddl = mvdef["schema"], mvdef["name"], mvdef["ddl"]
        tgt_fq = q(catalog_name, schema, mv_name)
        obj_fqn = fqn(catalog_name, schema, mv_name)
        print(f"\n  MV: {obj_fqn}")

        if not ddl:
            print(f"    SKIP: no DDL captured")
            write_log("materialized_views", "MATERIALIZED_VIEW", obj_fqn, "SKIPPED", "No DDL captured")
            continue

        t0 = time.time()
        try:
            # MVs must be created via SQL warehouse, not general compute
            ok, result = run_on_warehouse(ddl, sql_warehouse_id)
            if not ok:
                raise Exception(str(result))
            elapsed_create = time.time() - t0
            print(f"    Created via SQL warehouse ({elapsed_create:.1f}s)")

            # Grants and metadata can be applied from general compute
            if mvdef["comment"]:
                run_quiet(f"COMMENT ON TABLE {tgt_fq} IS '{escape_sql(mvdef['comment'])}'")
            if mvdef["tags"]:
                tag_pairs = ", ".join(f"'{t[0]}' = '{escape_sql(t[1])}'" for t in mvdef["tags"])
                run_quiet(f"ALTER TABLE {tgt_fq} SET TAGS ({tag_pairs})")

            gs, gf = apply_grants(mvdef["grants"], "TABLE", tgt_fq, "materialized_views", obj_fqn)
            set_owner("TABLE", tgt_fq, mvdef["owner"])

            elapsed = time.time() - t0
            write_log("materialized_views", "MATERIALIZED_VIEW", obj_fqn, "SUCCESS",
                      f"grants={gs}", elapsed)
            print(f"    OK (grants: {gs}) [{elapsed:.1f}s total]")

        except Exception as e:
            elapsed = time.time() - t0
            write_log("materialized_views", "MATERIALIZED_VIEW", obj_fqn, "FAILED", str(e), elapsed)
            print(f"    FAILED: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transfer Catalog Ownership
# MAGIC
# MAGIC Done last so the notebook runner retains admin access throughout the migration.

# COMMAND ----------

print("Transferring catalog ownership ...")
set_owner("CATALOG", q(catalog_name), source_info["owner"])
write_log("ownership", "CATALOG", catalog_name, "SUCCESS", f"owner={source_info['owner']}")
print(f"  Catalog '{catalog_name}' owner set to: {source_info['owner']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 9 — Validation

# COMMAND ----------

print("=" * 60)
print("VALIDATION")
print("=" * 60)

validation_passed = True

# ---------------------------------------------------------------------------
# 1. Object count comparison
# ---------------------------------------------------------------------------
print("\n--- Object Count Comparison ---")

checks = []

# Schemas
new_schemas = run(f"""
    SELECT schema_name FROM {q(catalog_name)}.information_schema.schemata
    WHERE schema_name != 'information_schema'
""").count()
bkp_schemas = run(f"""
    SELECT schema_name FROM {q(bkp_catalog)}.information_schema.schemata
    WHERE schema_name != 'information_schema'
""").count()
match = new_schemas == bkp_schemas
checks.append(("schemas_count", new_schemas, bkp_schemas, match))
if not match: validation_passed = False
print(f"  Schemas:              new={new_schemas}, original={bkp_schemas}  [{'PASS' if match else 'FAIL'}]")

# Managed tables (excluding internal MV/DLT backing tables)
new_tables_all = run(f"""
    SELECT table_name FROM {q(catalog_name)}.information_schema.tables
    WHERE table_schema != 'information_schema'
      AND table_type IN ('MANAGED', 'TABLE', 'BASE TABLE')
""").collect()
new_tables = len([r for r in new_tables_all if not is_internal_table(r.table_name)])
bkp_tables_count = len(managed_tables)
match = new_tables == bkp_tables_count
checks.append(("managed_tables_count", new_tables, bkp_tables_count, match))
if not match: validation_passed = False
print(f"  Managed tables:       new={new_tables}, original={bkp_tables_count}  [{'PASS' if match else 'FAIL'}]")

# Views
new_views = run(f"""
    SELECT table_name FROM {q(catalog_name)}.information_schema.tables
    WHERE table_schema != 'information_schema' AND table_type = 'VIEW'
""").count()
bkp_views_count = len(views)
match = new_views == bkp_views_count
checks.append(("views_count", new_views, bkp_views_count, match))
if not match: validation_passed = False
print(f"  Views:                new={new_views}, original={bkp_views_count}  [{'PASS' if match else 'FAIL'}]")

# Materialized views
new_mvs = run(f"""
    SELECT table_name FROM {q(catalog_name)}.information_schema.tables
    WHERE table_schema != 'information_schema' AND table_type = 'MATERIALIZED_VIEW'
""").count()
bkp_mvs_count = len(materialized_views)
match = new_mvs == bkp_mvs_count
checks.append(("materialized_views_count", new_mvs, bkp_mvs_count, match))
if not match: validation_passed = False
print(f"  Materialized views:   new={new_mvs}, original={bkp_mvs_count}  [{'PASS' if match else 'FAIL'}]")

# Functions
ok_fn_new, fn_new_df = run_quiet(f"""
    SELECT routine_name FROM {q(catalog_name)}.information_schema.routines
    WHERE routine_schema != 'information_schema'
""")
new_fns = fn_new_df.count() if ok_fn_new else 0
bkp_fns_count = len(functions)
match = new_fns == bkp_fns_count
checks.append(("functions_count", new_fns, bkp_fns_count, match))
if not match: validation_passed = False
print(f"  Functions:            new={new_fns}, original={bkp_fns_count}  [{'PASS' if match else 'FAIL'}]")

# Volumes
ok_vol_new, vol_new_df = run_quiet(f"""
    SELECT volume_name FROM {q(catalog_name)}.information_schema.volumes
    WHERE volume_schema != 'information_schema' AND volume_type = 'MANAGED'
""")
new_vols = vol_new_df.count() if ok_vol_new else 0
bkp_vols_count = len(managed_volumes)
match = new_vols == bkp_vols_count
checks.append(("managed_volumes_count", new_vols, bkp_vols_count, match))
if not match: validation_passed = False
print(f"  Managed volumes:      new={new_vols}, original={bkp_vols_count}  [{'PASS' if match else 'FAIL'}]")

# Log object count checks
for check_name, new_val, orig_val, passed in checks:
    status = "PASS" if passed else "FAIL"
    write_log("validation", "COUNT_CHECK", check_name, status,
              f"new={new_val} original={orig_val}")

# ---------------------------------------------------------------------------
# 2. Row count comparison for managed tables
# ---------------------------------------------------------------------------
print("\n--- Row Count Comparison (Managed Tables) ---")

for tbl in managed_tables:
    schema, table = tbl.table_schema, tbl.table_name
    obj_fqn = fqn(catalog_name, schema, table)
    try:
        new_count = run(f"SELECT COUNT(*) FROM {q(catalog_name, schema, table)}").first()[0]
        bkp_count = run(f"SELECT COUNT(*) FROM {q(bkp_catalog, schema, table)}").first()[0]
        match = new_count == bkp_count
        status = "PASS" if match else "FAIL"
        if not match:
            validation_passed = False
        write_log("validation", "ROW_COUNT", obj_fqn, status,
                  f"new={new_count} original={bkp_count}")
        print(f"  {schema}.{table}: new={new_count}, original={bkp_count}  [{status}]")
    except Exception as e:
        validation_passed = False
        write_log("validation", "ROW_COUNT", obj_fqn, "ERROR", str(e))
        print(f"  {schema}.{table}: ERROR — {e}")

# ---------------------------------------------------------------------------
# 3. Grant comparison for catalog and schemas
# ---------------------------------------------------------------------------
print("\n--- Grant Comparison (Catalog & Schemas) ---")

new_cat_grants = set((g[0], g[1]) for g in get_grants("CATALOG", q(catalog_name)))
bkp_cat_grants = set((g[0], g[1]) for g in get_grants("CATALOG", q(bkp_catalog)))
new_cat_grants = {g for g in new_cat_grants if g[1].upper() not in ("OWN", "OWNERSHIP")}
bkp_cat_grants = {g for g in bkp_cat_grants if g[1].upper() not in ("OWN", "OWNERSHIP")}
missing = bkp_cat_grants - new_cat_grants
if missing:
    validation_passed = False
    write_log("validation", "GRANT_CHECK", catalog_name, "FAIL", f"missing={missing}")
    print(f"  Catalog grants — MISSING in new: {missing}")
else:
    write_log("validation", "GRANT_CHECK", catalog_name, "PASS", f"{len(new_cat_grants)} grants")
    print(f"  Catalog grants — PASS ({len(new_cat_grants)} grants)")

for schema_name in schemas:
    new_s_grants = set((g[0], g[1]) for g in get_grants("SCHEMA", q(catalog_name, schema_name)))
    bkp_s_grants = set((g[0], g[1]) for g in get_grants("SCHEMA", q(bkp_catalog, schema_name)))
    new_s_grants = {g for g in new_s_grants if g[1].upper() not in ("OWN", "OWNERSHIP")}
    bkp_s_grants = {g for g in bkp_s_grants if g[1].upper() not in ("OWN", "OWNERSHIP")}
    missing = bkp_s_grants - new_s_grants
    if missing:
        validation_passed = False
        write_log("validation", "GRANT_CHECK", f"{catalog_name}.{schema_name}", "FAIL", f"missing={missing}")
        print(f"  Schema {schema_name} grants — MISSING: {missing}")
    else:
        write_log("validation", "GRANT_CHECK", f"{catalog_name}.{schema_name}", "PASS",
                  f"{len(new_s_grants)} grants")
        print(f"  Schema {schema_name} grants — PASS ({len(new_s_grants)} grants)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migration Summary

# COMMAND ----------

# Query log table for summary
print("=" * 60)
print(f"MIGRATION SUMMARY  (batch_id: {batch_id})")
print("=" * 60)

summary_df = run(f"""
    SELECT phase, object_type, status, COUNT(*) as cnt
    FROM {LOG_TABLE}
    WHERE batch_id = '{batch_id}'
    GROUP BY phase, object_type, status
    ORDER BY phase, object_type, status
""")
summary_df.show(100, truncate=False)

# Show failures
failures_df = run(f"""
    SELECT phase, object_type, object_name, message
    FROM {LOG_TABLE}
    WHERE batch_id = '{batch_id}' AND status IN ('FAILED', 'ERROR')
    ORDER BY phase, object_name
""")
failure_count = failures_df.count()

if failure_count > 0:
    print(f"\n--- FAILURES ({failure_count}) ---")
    failures_df.show(100, truncate=False)

print(f"\n{'=' * 60}")
if validation_passed and failure_count == 0:
    print("RESULT: ALL CHECKS PASSED — MIGRATION SUCCESSFUL")
elif validation_passed:
    print("RESULT: VALIDATION PASSED WITH SOME MIGRATION WARNINGS")
else:
    print("RESULT: SOME CHECKS FAILED — REVIEW OUTPUT ABOVE")
print(f"{'=' * 60}")

print(f"""
Batch ID: {batch_id}
Log table: {LOG_TABLE}

Query full log:
  SELECT * FROM {LOG_TABLE} WHERE batch_id = '{batch_id}' ORDER BY logged_at

Next steps:
  1. Verify dashboards and workflows that depend on '{catalog_name}' still work.
  2. If Delta Sharing was flagged, update share references manually.
  3. If DLT pipelines target this catalog, trigger a full refresh.
  4. Once validated, the backup catalog '{bkp_catalog}' can be dropped:
     DROP CATALOG {q(bkp_catalog)} CASCADE;
""")

if validation_passed:
    dbutils.notebook.exit(f"MIGRATION_COMPLETE|batch_id={batch_id}")
else:
    dbutils.notebook.exit(f"MIGRATION_COMPLETE_WITH_WARNINGS|batch_id={batch_id}")
