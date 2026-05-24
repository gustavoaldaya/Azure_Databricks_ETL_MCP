# Entity: skill — databricks-medallion-etl

- **Type:** Skill (reusable, cross-project)
- **Created:** 2026-05-24
- **Purpose:** Auto-discovery medallion ETL on Azure Databricks from a storage path param.

## Mechanism split
- Discovery: Azure MCP (storage tools).
- Launch: Databricks CLI/SDK (`run-now`), polled.

## Parameters
source_path, source_format(auto), catalog, schema_prefix, table_name,
checkpoint_root, discovery_engine(auto_loader|plain_read), primary_keys, write_mode.

## Tables produced
- <catalog>.<prefix>_bronze.<table>
- <catalog>.<prefix>_silver.<table>
- <catalog>.<prefix>_gold.<table>_summary

## Validation status
Transform core validated locally on synthetic data (2026-05-24). Live-workspace
run pending one-time job creation (references/setup.md).

## Related
- [[ADR-001-azure-mcp-discovery-only]]
