# Databricks notebook source
# MAGIC %md
# MAGIC # Medallion ETL — Auto-discovery + Bronze/Silver/Gold
# MAGIC
# MAGIC Parametrized notebook. Receives a storage `source_path` and materializes the
# MAGIC three medallion layers into Unity Catalog. Schema is *discovered* from the
# MAGIC source (Auto Loader `cloudFiles` with inference + evolution; falls back to a
# MAGIC plain `spark.read` when serverless / DBR requirements are not met).
# MAGIC
# MAGIC This notebook is the *execution* artifact. It is triggered as a Databricks Job.
# MAGIC Discovery/validation of the storage path and job launch are orchestrated
# MAGIC externally (Azure MCP for storage; Databricks CLI/SDK for the job run).

# COMMAND ----------
# MAGIC %md ## Widgets (parameters)

# COMMAND ----------
dbutils.widgets.text("source_path", "", "Source storage path (abfss://...)")
dbutils.widgets.text("source_format", "auto", "csv|json|parquet|delta|auto")
dbutils.widgets.text("catalog", "main", "Unity Catalog catalog")
dbutils.widgets.text("schema_prefix", "medallion", "Schema/dataset name prefix")
dbutils.widgets.text("table_name", "dataset", "Logical table name for the layers")
dbutils.widgets.text("checkpoint_root", "", "Checkpoint/schema location root (abfss://...)")
dbutils.widgets.dropdown("discovery_engine", "auto_loader", ["auto_loader", "plain_read"], "Discovery engine")
dbutils.widgets.text("primary_keys", "", "Comma-separated PK columns for Silver dedup (optional)")
dbutils.widgets.dropdown("write_mode", "incremental", ["incremental", "full_refresh"], "Write mode")

P = {k: dbutils.widgets.get(k) for k in [
    "source_path", "source_format", "catalog", "schema_prefix", "table_name",
    "checkpoint_root", "discovery_engine", "primary_keys", "write_mode",
]}

assert P["source_path"], "source_path is required"
if not P["checkpoint_root"]:
    # Default the checkpoint under the source path's parent if not provided.
    P["checkpoint_root"] = P["source_path"].rstrip("/") + "/_checkpoints"

print("Parameters:")
for k, v in P.items():
    print(f"  {k} = {v!r}")

# COMMAND ----------
# MAGIC %md ## Imports & helpers

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from datetime import datetime, timezone

BRONZE = f"{P['catalog']}.{P['schema_prefix']}_bronze.{P['table_name']}"
SILVER = f"{P['catalog']}.{P['schema_prefix']}_silver.{P['table_name']}"
GOLD   = f"{P['catalog']}.{P['schema_prefix']}_gold.{P['table_name']}_summary"

for layer in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {P['catalog']}.{P['schema_prefix']}_{layer}")

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def detect_format(path: str, declared: str) -> str:
    """Resolve the source format. If 'auto', sniff by file extension listing."""
    if declared and declared != "auto":
        return declared
    try:
        files = dbutils.fs.ls(path)
    except Exception as e:
        raise RuntimeError(f"Cannot list source_path {path}: {e}")
    exts = {f.name.rsplit(".", 1)[-1].lower() for f in files if "." in f.name}
    for fmt in ("delta", "parquet", "json", "csv"):
        if fmt in exts or (fmt == "delta" and any(f.name == "_delta_log/" for f in files)):
            return fmt
    # Default: assume csv with header (most common ambiguous case)
    return "csv"


# COMMAND ----------
# MAGIC %md ## Bronze — raw ingest with lineage (schema discovery happens here)

# COMMAND ----------
fmt = detect_format(P["source_path"], P["source_format"])
print(f"Resolved source format: {fmt}")

LINEAGE_COLS = lambda df: (
    df.withColumn("_ingest_ts", F.current_timestamp())
      .withColumn("_ingest_run_id", F.lit(RUN_ID))
      .withColumn("_source_file", F.col("_metadata.file_path"))
)

if P["discovery_engine"] == "auto_loader":
    # Auto Loader: native schema inference + evolution. Requires schema location.
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", fmt)
        .option("cloudFiles.schemaLocation", f"{P['checkpoint_root']}/bronze_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    )
    if fmt == "csv":
        reader = reader.option("header", "true")
    src = LINEAGE_COLS(reader.load(P["source_path"]))

    bronze_query = (
        src.writeStream.format("delta")
        .option("checkpointLocation", f"{P['checkpoint_root']}/bronze_write")
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)            # batch-like run, then stop
        .toTable(BRONZE)
    )
    bronze_query.awaitTermination()
    print(f"Bronze (Auto Loader) -> {BRONZE}")
else:
    # Plain read fallback: simpler, portable, no streaming/serverless requirement.
    reader = spark.read.format(fmt)
    if fmt == "csv":
        reader = reader.option("header", "true").option("inferSchema", "true")
    elif fmt == "json":
        reader = reader.option("inferSchema", "true")
    src = LINEAGE_COLS(reader.load(P["source_path"]))
    mode = "overwrite" if P["write_mode"] == "full_refresh" else "append"
    (src.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(BRONZE))
    print(f"Bronze (plain read) -> {BRONZE}")

# Persist the discovered schema as an artifact for the orchestrator / KB.
discovered_schema = spark.table(BRONZE).schema.jsonValue()
print("Discovered schema columns:",
      [f["name"] for f in discovered_schema["fields"]])

# COMMAND ----------
# MAGIC %md ## Silver — typed cleanup + dedup + quality, driven by discovered schema

# COMMAND ----------
bronze_df = spark.table(BRONZE)

# Drop fully-null columns, trim strings, standardize empty strings to null.
string_cols = [f.name for f in bronze_df.schema.fields
               if f.dataType.simpleString() == "string"
               and not f.name.startswith("_")]
silver_df = bronze_df
for c in string_cols:
    silver_df = silver_df.withColumn(
        c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c)))
    )

# Dedup: by declared PKs if given, else by full business-column tuple.
pks = [c.strip() for c in P["primary_keys"].split(",") if c.strip()]
if pks:
    w_cols = pks
else:
    w_cols = [c for c in silver_df.columns if not c.startswith("_")]
silver_df = silver_df.dropDuplicates(w_cols)

# Quality flag column: rows missing any PK are marked rather than dropped.
if pks:
    null_pk = None
    for c in pks:
        cond = F.col(c).isNull()
        null_pk = cond if null_pk is None else (null_pk | cond)
    silver_df = silver_df.withColumn("_dq_pk_complete", ~null_pk)

(silver_df.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(SILVER))
print(f"Silver -> {SILVER}  ({silver_df.count()} rows)")

# COMMAND ----------
# MAGIC %md ## Gold — generic aggregate summary (override per-dataset as needed)

# COMMAND ----------
silver = spark.table(SILVER)
numeric_cols = [f.name for f in silver.schema.fields
                if f.dataType.simpleString() in ("double", "float", "int", "bigint", "long", "decimal")
                and not f.name.startswith("_")]

if numeric_cols:
    aggs = []
    for c in numeric_cols:
        aggs += [F.count(c).alias(f"{c}__count"),
                 F.avg(c).alias(f"{c}__avg"),
                 F.min(c).alias(f"{c}__min"),
                 F.max(c).alias(f"{c}__max")]
    gold_df = silver.agg(*aggs).withColumn("_run_id", F.lit(RUN_ID))
else:
    gold_df = (silver.agg(F.count(F.lit(1)).alias("row_count"))
               .withColumn("_run_id", F.lit(RUN_ID)))

(gold_df.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(GOLD))
print(f"Gold -> {GOLD}")

# COMMAND ----------
# MAGIC %md ## Return summary for the orchestrator

# COMMAND ----------
import json
result = {
    "run_id": RUN_ID,
    "source_path": P["source_path"],
    "resolved_format": fmt,
    "discovery_engine": P["discovery_engine"],
    "bronze_table": BRONZE,
    "silver_table": SILVER,
    "gold_table": GOLD,
    "discovered_columns": [f["name"] for f in discovered_schema["fields"]],
}
dbutils.notebook.exit(json.dumps(result))
