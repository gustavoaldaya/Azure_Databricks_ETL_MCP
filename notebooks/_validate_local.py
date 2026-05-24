"""
Local validation of the medallion transform logic (no Databricks, no Azure).
Replicates the Bronze->Silver->Gold core from notebooks/medallion_etl.py against
synthetic CSV data to prove the discovery + dedup + quality + aggregate logic.
"""
import os, tempfile, json
from pyspark.sql import SparkSession, functions as F
from datetime import datetime, timezone

spark = (SparkSession.builder
         .appName("medallion-local-validate")
         .master("local[2]")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
tmp = tempfile.mkdtemp()
src = os.path.join(tmp, "raw")
os.makedirs(src, exist_ok=True)

# Synthetic raw CSV: includes dupes, blank strings, a null PK, a numeric col.
csv = """id,name,region,amount
1, Alice , EU,100.5
2,Bob,US,200.0
2,Bob,US,200.0
3,  ,EU,
,Ghost,APAC,50.0
4,Dana,US,300.25
"""
with open(os.path.join(src, "part-001.csv"), "w") as f:
    f.write(csv)

print("=== BRONZE (plain read fallback path) ===")
bronze = (spark.read.format("csv").option("header", "true").option("inferSchema", "true")
          .load(src)
          .withColumn("_ingest_ts", F.current_timestamp())
          .withColumn("_ingest_run_id", F.lit(RUN_ID)))
bronze.show(truncate=False)
discovered = [f.name for f in bronze.schema.fields]
print("Discovered columns:", discovered)
assert {"id", "name", "region", "amount"}.issubset(set(discovered)), "schema discovery failed"

print("=== SILVER (trim, blank->null, dedup, DQ flag) ===")
string_cols = [f.name for f in bronze.schema.fields
               if f.dataType.simpleString() == "string" and not f.name.startswith("_")]
silver = bronze
for c in string_cols:
    silver = silver.withColumn(c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c))))
pks = ["id"]
silver = silver.dropDuplicates(pks)
null_pk = F.col("id").isNull()
silver = silver.withColumn("_dq_pk_complete", ~null_pk)
silver.select("id", "name", "region", "amount", "_dq_pk_complete").show(truncate=False)

rows = silver.count()
print("Silver row count:", rows)
# Expect: dupe id=2 collapsed, null-PK ghost row retained but flagged. 5 distinct ids incl null.
assert rows == 5, f"expected 5 deduped rows, got {rows}"
flagged = silver.filter(~F.col("_dq_pk_complete")).count()
assert flagged == 1, f"expected 1 DQ-flagged row, got {flagged}"
trimmed = silver.filter(F.col("name") == "Alice").count()
assert trimmed == 1, "trim of ' Alice ' failed"
blanked = silver.filter((F.col("id") == 3) & F.col("name").isNull()).count()
assert blanked == 1, "blank-string->null failed"

print("=== GOLD (generic numeric aggregate) ===")
numeric_cols = [f.name for f in silver.schema.fields
                if f.dataType.simpleString() in ("double", "float", "int", "bigint", "long")
                and not f.name.startswith("_")]
print("Numeric cols for aggregation:", numeric_cols)
aggs = []
for c in [c for c in numeric_cols if c == "amount"]:
    aggs += [F.count(c).alias(f"{c}__count"), F.avg(c).alias(f"{c}__avg"),
             F.min(c).alias(f"{c}__min"), F.max(c).alias(f"{c}__max")]
gold = silver.agg(*aggs).withColumn("_run_id", F.lit(RUN_ID))
gold.show(truncate=False)
gold_row = gold.collect()[0]
assert gold_row["amount__count"] == 4, "gold count mismatch"

result = {"run_id": RUN_ID, "discovered_columns": discovered,
          "silver_rows": rows, "dq_flagged": flagged}
print("=== RESULT ===")
print(json.dumps(result, indent=2))
print("\nALL ASSERTIONS PASSED ✓")
spark.stop()
