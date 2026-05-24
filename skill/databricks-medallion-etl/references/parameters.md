# Parameter & Contract Reference

## Notebook widgets (medallion_etl.py)

| Widget | Default | Description |
|--------|---------|-------------|
| `source_path` | *(required)* | ADLS Gen2 path, e.g. `abfss://raw@acct.dfs.core.windows.net/sales/2026/` |
| `source_format` | `auto` | `csv` \| `json` \| `parquet` \| `delta` \| `auto` (sniffs by extension) |
| `catalog` | `main` | Unity Catalog catalog |
| `schema_prefix` | `medallion` | Schemas created: `<prefix>_bronze/_silver/_gold` |
| `table_name` | `dataset` | Logical table name across the three layers |
| `checkpoint_root` | *(derived)* | abfss root for Auto Loader schema + write checkpoints |
| `discovery_engine` | `auto_loader` | `auto_loader` \| `plain_read` |
| `primary_keys` | `""` | Comma-separated PK columns for Silver dedup + DQ flag |
| `write_mode` | `incremental` | `incremental` (append) \| `full_refresh` (overwrite) |

Resulting tables:
- `<catalog>.<prefix>_bronze.<table_name>`
- `<catalog>.<prefix>_silver.<table_name>`
- `<catalog>.<prefix>_gold.<table_name>_summary`

## Discovery contract (Azure MCP -> orchestrator)

The agent fills this from Azure MCP storage tool results and saves as JSON:

| Field | Type | Source |
|-------|------|--------|
| `storage_account` | str | Azure MCP "get storage account details" |
| `container` | str | container name |
| `prefix` | str | folder path inside the container |
| `abfss_path` | str | composed `abfss://<container>@<account>.dfs.core.windows.net/<prefix>` |
| `files_found` | int | count from blob listing — MUST be > 0 |
| `detected_formats` | list[str] | file extensions observed |

`orchestrate_medallion.py` validates: `files_found > 0` and `abfss_path`
starts with `abfss://`. On failure it exits non-zero before any job launch.

## Orchestrator CLI flags
See `scripts/orchestrate_medallion.py --help`. Key ones mirror the widgets;
`--job-id` and `--discovery-json` are required. `--no-poll` submits without
waiting.
