---
name: databricks-medallion-etl
description: >-
  Build and run a medallion (Bronze/Silver/Gold) ETL on Azure Databricks that
  auto-discovers the schema from a storage path given as a parameter. Use this
  skill WHENEVER the user wants to ingest data from an Azure storage path (ADLS
  Gen2 / abfss) into Databricks, build or refresh medallion layers, do schema
  discovery/inference over a data lake folder, or set up an unattended
  Bronze/Silver/Gold pipeline. Trigger even if the user just says "ingest this
  container", "discover the schema and load it", "build the medallion layers",
  or names a storage path. Discovery of the storage path is done via Azure MCP;
  the medallion job is launched via the Databricks CLI/SDK (Azure MCP does NOT
  expose Databricks tools).
---

# Databricks Medallion ETL (auto-discovery)

Materializes Bronze/Silver/Gold layers in Unity Catalog from a parameter-supplied
Azure storage path, discovering the schema automatically (Auto Loader by default).

## The division of labor (read this first)

The Azure MCP Server v1.0 exposes **Azure Storage** tools but **no Databricks
tools**. Therefore this skill splits the work:

| Phase | Mechanism | What happens |
|-------|-----------|--------------|
| 1. Discovery | **Azure MCP** (storage tools) | Resolve storage account, validate the path/container, list files, sniff formats |
| 2. ETL run | **Databricks Job** (notebook) | Auto Loader discovers schema; Bronze→Silver→Gold are written to Unity Catalog |
| 3. Launch | **Databricks CLI/SDK** | `run-now` the parametrized job; poll to completion |

Do NOT claim Azure MCP "runs the ETL". It validates storage and nothing more on
the Databricks side.

## Workflow

### Step 1 — Discover & validate the path (Azure MCP)
Using the connected Azure MCP server, the agent issues prompts such as:
- "Get details about my storage account `<account>`"
- "List the blobs under `<prefix>` in container `<container>`"

Collect the results into the **discovery contract** (see
`references/parameters.md`) and write them to a small JSON file, e.g.:
```json
{
  "storage_account": "mydatalake",
  "container": "raw",
  "prefix": "sales/2026/",
  "abfss_path": "abfss://raw@mydatalake.dfs.core.windows.net/sales/2026/",
  "files_found": 12,
  "detected_formats": ["csv"]
}
```
If `files_found` is 0, STOP and report — do not launch a job against an empty path.

### Step 2 — Ensure the job exists
The notebook `assets/medallion_etl.py` must be uploaded to the workspace and wired
to a Databricks Job (one-time setup). See `references/setup.md` for the
`databricks jobs create` definition. Note the resulting `job_id`.

### Step 3 — Launch (Databricks SDK/CLI)
```bash
python scripts/orchestrate_medallion.py \
  --job-id <JOB_ID> \
  --discovery-json discovery.json \
  --catalog main --schema-prefix medallion --table-name sales \
  --primary-keys id \
  --discovery-engine auto_loader \
  --write-mode incremental
```
The script triggers `run-now`, polls the run, and returns the notebook output
(discovered columns + the three table names).

## Discovery engine choice
- **`auto_loader`** (default): native schema inference + evolution
  (`cloudFiles`). Requires serverless or a recent DBR and a writable
  `checkpoint_root`. Best for recurring/incremental ingestion.
- **`plain_read`** (fallback): a single `spark.read` with `inferSchema`. Simpler
  and portable; use when serverless isn't available or for one-off loads.

If unsure which applies, prefer `auto_loader` and fall back to `plain_read` on
a serverless/DBR error.

## What each layer does
- **Bronze**: raw 1:1 ingest + lineage columns (`_ingest_ts`, `_ingest_run_id`,
  `_source_file`). Schema discovered here.
- **Silver**: typed cleanup — trim strings, empty→null, dedup (by `primary_keys`
  if given, else full business tuple), DQ flag `_dq_pk_complete` for rows with
  null PKs (flagged, not dropped).
- **Gold**: generic numeric aggregate summary. Override per dataset for real
  business marts (the generic version is a safe default, not a final design).

## Auth
Configure once: `databricks auth login --host https://<workspace-host>`
(OAuth, Entra ID compatible) or set `DATABRICKS_HOST` / `DATABRICKS_TOKEN`.
Azure MCP auth is handled by your Azure CLI / Entra login in the agent's runtime.

## Files in this skill
- `assets/medallion_etl.py` — the parametrized Databricks notebook (execution).
- `scripts/orchestrate_medallion.py` — discovery contract + SDK job launcher.
- `references/parameters.md` — full parameter & contract reference.
- `references/setup.md` — one-time job creation + workspace wiring.

## Limitations & caveats
- Azure MCP cannot trigger Databricks; the CLI/SDK path is mandatory for launch.
- The generic Gold layer is a placeholder for real marts.
- Auto Loader needs a persistent `checkpoint_root` per dataset; do not share
  checkpoints across datasets.
- Validate the transform logic locally with `_validate_local.py` before wiring
  to a live workspace (it runs the Bronze→Silver→Gold core on synthetic data).
