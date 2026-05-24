# ADR-001 — Azure MCP for discovery only; Databricks CLI/SDK for job launch

- **Status:** Accepted
- **Date:** 2026-05-24
- **Project:** Azure_Databricks_ETL_MCP

## Context
Goal: an ETL on Azure Databricks that auto-discovers the schema from a
parameter-supplied storage path and builds the three medallion layers. Initial
intent was to drive everything "via the Azure MCP".

## Decision
Verified against the Azure MCP Server v1.0 (GA) service list: it exposes Azure
Storage tools but **no Azure Databricks tools** (no workspace/cluster/job).
Therefore:
- **Discovery** (resolve storage account, validate path/container, list files,
  sniff formats) is done **via Azure MCP storage tools**.
- **Job launch** (run the medallion notebook) is done **via the Databricks
  CLI/SDK** (`run-now`), which is deterministic, auditable and unattended-friendly.

## Consequences
- The skill must NOT claim Azure MCP runs the ETL.
- A one-time job-creation step is required (notebook uploaded + Job wired).
- Discovery output is captured as a JSON contract that the orchestrator validates
  (files_found > 0, abfss path) before any launch.
- Schema discovery uses Auto Loader (cloudFiles) by default, with a plain
  spark.read fallback when serverless/DBR is unavailable.

## Alternatives considered
- "Azure CLI Generate" tool in Azure MCP: generates CLI text, does not execute
  Databricks — rejected.
- Databricks managed MCP servers (Genie/SQL/UC Functions): live inside the
  workspace for data access, not for launching external ETL jobs — out of scope
  for the launch path, though the SQL one could express layers as SQL/DLT later.

## Validation
Bronze→Silver→Gold transform logic validated locally on synthetic CSV
(dedup, blank→null, trim, DQ flag, numeric aggregates) — all assertions passed,
no Azure/Databricks connection required.
