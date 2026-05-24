#!/usr/bin/env python3
"""
orchestrate_medallion.py
========================
Drives the medallion ETL end to end. Two responsibilities, two mechanisms:

  1) DISCOVERY  -> Azure MCP (storage). The *agent* calls Azure MCP storage tools
     to resolve the storage account, list the container, and validate that
     `source_path` exists and contains files. Azure MCP does NOT expose Databricks
     tools, so it is used ONLY for the storage side.

  2) JOB LAUNCH -> Databricks SDK/CLI. The medallion notebook is triggered as a
     Databricks Job run (run-now), parameters passed as widgets. This is the
     deterministic, auditable, unattended path.

This script is the SDK half (step 2) plus a thin discovery contract (step 1) that
the orchestrating agent fills in from Azure MCP results. When run standalone it
expects the discovery payload to be provided (path already validated by the agent).

Auth: relies on a configured Databricks CLI profile (`databricks auth login`) or
the standard DATABRICKS_HOST / DATABRICKS_TOKEN env vars.
"""
from __future__ import annotations
import argparse, json, sys, time
from dataclasses import dataclass, asdict


# ---------------------------------------------------------------------------
# Step 1 contract: what the agent must obtain from Azure MCP before launching.
# ---------------------------------------------------------------------------
@dataclass
class DiscoveryResult:
    """Filled by the orchestrating agent using Azure MCP storage tools.

    Suggested Azure MCP prompts (the agent issues these, not this script):
      - "Get details about my storage account '<account>'"
      - "Get details about my Storage container '<container>'"
      - "List the blobs under path '<prefix>' in container '<container>'"
    """
    storage_account: str
    container: str
    prefix: str
    abfss_path: str           # abfss://<container>@<account>.dfs.core.windows.net/<prefix>
    files_found: int
    detected_formats: list[str]

    def validate(self) -> None:
        if self.files_found <= 0:
            raise SystemExit(f"Discovery found no files under {self.abfss_path}")
        if not self.abfss_path.startswith("abfss://"):
            raise SystemExit(f"Expected abfss:// path, got {self.abfss_path!r}")


# ---------------------------------------------------------------------------
# Step 2: launch the Databricks Job that runs notebooks/medallion_etl.py
# ---------------------------------------------------------------------------
def launch_job(job_id: int, params: dict, poll: bool = True) -> dict:
    """Trigger an existing Databricks Job by id with notebook params, then poll."""
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.jobs import RunNow
    except ImportError:
        raise SystemExit(
            "databricks-sdk not installed. Run: pip install databricks-sdk"
        )

    w = WorkspaceClient()  # picks up profile / env vars automatically
    run = w.jobs.run_now(job_id=job_id, notebook_params=params)
    run_id = run.response.run_id if hasattr(run, "response") else run.run_id
    print(f"[launch] job_id={job_id} run_id={run_id}")

    if not poll:
        return {"run_id": run_id, "status": "SUBMITTED"}

    while True:
        info = w.jobs.get_run(run_id=run_id)
        state = info.state
        life = state.life_cycle_state.value if state and state.life_cycle_state else "?"
        print(f"[poll] run_id={run_id} life_cycle={life}")
        if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            result_state = state.result_state.value if state.result_state else "?"
            output = None
            try:
                out = w.jobs.get_run_output(run_id=run_id)
                output = out.notebook_output.result if out.notebook_output else None
            except Exception:
                pass
            return {"run_id": run_id, "life_cycle": life,
                    "result_state": result_state, "notebook_output": output}
        time.sleep(15)


def build_params(discovery: DiscoveryResult, args) -> dict:
    return {
        "source_path": discovery.abfss_path,
        "source_format": args.source_format,
        "catalog": args.catalog,
        "schema_prefix": args.schema_prefix,
        "table_name": args.table_name,
        "checkpoint_root": args.checkpoint_root or "",
        "discovery_engine": args.discovery_engine,
        "primary_keys": args.primary_keys or "",
        "write_mode": args.write_mode,
    }


def main(argv=None):
    ap = argparse.ArgumentParser(description="Launch the medallion ETL Databricks Job.")
    ap.add_argument("--job-id", type=int, required=True, help="Databricks Job id to run-now")
    ap.add_argument("--discovery-json", required=True,
                    help="Path to JSON file with the Azure MCP discovery result")
    ap.add_argument("--source-format", default="auto")
    ap.add_argument("--catalog", default="main")
    ap.add_argument("--schema-prefix", default="medallion")
    ap.add_argument("--table-name", default="dataset")
    ap.add_argument("--checkpoint-root", default="")
    ap.add_argument("--discovery-engine", default="auto_loader",
                    choices=["auto_loader", "plain_read"])
    ap.add_argument("--primary-keys", default="")
    ap.add_argument("--write-mode", default="incremental",
                    choices=["incremental", "full_refresh"])
    ap.add_argument("--no-poll", action="store_true")
    args = ap.parse_args(argv)

    with open(args.discovery_json) as f:
        disc = DiscoveryResult(**json.load(f))
    disc.validate()
    print("[discovery] validated:", json.dumps(asdict(disc), indent=2))

    params = build_params(disc, args)
    print("[params]", json.dumps(params, indent=2))

    result = launch_job(args.job_id, params, poll=not args.no_poll)
    print("[done]", json.dumps(result, indent=2))
    if result.get("result_state") not in (None, "SUCCESS"):
        sys.exit(2)


if __name__ == "__main__":
    main()
