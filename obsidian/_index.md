# Azure_Databricks_ETL_MCP — Index

Project to build a reusable **skill** (`databricks-medallion-etl`) that runs an
auto-discovery medallion ETL on Azure Databricks.

## Status (2026-05-24)
- Skill drafted and locally validated. Not yet wired to a live workspace.

## Key decisions
- [[ADR-001-azure-mcp-discovery-only]] — Azure MCP = discovery (storage) only;
  Databricks CLI/SDK launches the job. Azure MCP v1.0 has no Databricks tools.

## Confirmed design choices
- Job launch: via Azure MCP for discovery + **Databricks SDK** for the run.
- Discovery engine: **Auto Loader (cloudFiles)**, fallback `plain_read`.
- Registry: this Obsidian project.

## Components
- `skill/databricks-medallion-etl/SKILL.md` — skill entry point.
- `assets/medallion_etl.py` — parametrized notebook (Bronze/Silver/Gold).
- `scripts/orchestrate_medallion.py` — discovery contract + SDK launcher.
- `references/parameters.md`, `references/setup.md`.

## Open questions
- Gold layer is generic; needs per-dataset mart design.
- Confirm serverless availability in the target workspace (Auto Loader).
- Verify exact Azure MCP storage tool names/params against the live connector.

## Entities
- [[entities/skill-databricks-medallion-etl]]
