# Azure_Databricks_ETL_MCP

Auto-discovery **medallion (Bronze / Silver / Gold) ETL** for Azure Databricks,
packaged as a reusable skill. Given a storage path as a parameter, it discovers
the schema and materializes the three medallion layers into Unity Catalog.

## How it works

| Phase | Mechanism | Role |
|-------|-----------|------|
| 1. Discovery | **Azure MCP** (storage tools) | Resolve storage account, validate path/container, list files, sniff format |
| 2. ETL run | **Databricks Job** (notebook) | Auto Loader discovers schema; Bronze→Silver→Gold written to Unity Catalog |
| 3. Launch | **Databricks CLI/SDK** | `run-now` the parametrized job; poll to completion |

> **Note:** The Azure MCP Server v1.0 exposes Azure Storage tools but **no
> Databricks tools**. Discovery is done via Azure MCP; the job is launched via
> the Databricks SDK/CLI. See `obsidian/adrs/ADR-001-azure-mcp-discovery-only.md`.

## Layout

```
skill/databricks-medallion-etl/   # the reusable skill
  SKILL.md                        #   entry point + workflow
  assets/medallion_etl.py         #   parametrized Databricks notebook
  assets/_validate_local.py       #   local transform validation (synthetic data)
  scripts/orchestrate_medallion.py#   discovery contract + SDK job launcher
  references/parameters.md        #   parameter & contract reference
  references/setup.md             #   one-time job creation + auth
notebooks/                        # working copies of the notebook + validator
obsidian/                         # decision records (AgentMemory-style)
  _index.md  adrs/  entities/
```

## Quickstart

```bash
# 1. validate the transform logic locally (no Azure/Databricks needed)
python notebooks/_validate_local.py

# 2. one-time: upload notebook + create the Job (see references/setup.md)

# 3. run: discovery JSON (from Azure MCP) + SDK launch
python skill/databricks-medallion-etl/scripts/orchestrate_medallion.py \
  --job-id <JOB_ID> --discovery-json discovery.json \
  --catalog main --schema-prefix medallion --table-name sales --primary-keys id
```

## Discovery engine
- `auto_loader` (default): Auto Loader `cloudFiles` — schema inference + evolution.
- `plain_read` (fallback): single `spark.read` with `inferSchema`.

## Status
Skill drafted; Bronze→Silver→Gold core validated locally. Live-workspace wiring
pending one-time job creation.
