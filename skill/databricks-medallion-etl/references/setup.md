# One-time Setup

## 1. Upload the notebook
```bash
databricks workspace import \
  ./assets/medallion_etl.py \
  /Workspace/Shared/medallion_etl \
  --language PYTHON --format SOURCE --overwrite
```

## 2. Create the Job
Minimal `job.json`:
```json
{
  "name": "medallion-etl",
  "tasks": [
    {
      "task_key": "medallion",
      "notebook_task": {
        "notebook_path": "/Workspace/Shared/medallion_etl",
        "source": "WORKSPACE"
      },
      "environment_key": "serverless-env"
    }
  ],
  "environments": [
    { "environment_key": "serverless-env",
      "spec": { "client": "3" } }
  ]
}
```
```bash
databricks jobs create --json @job.json
# -> note the returned job_id
```
Serverless is recommended so Auto Loader works out of the box. For classic
compute, replace `environment_key` with a `job_cluster` definition (DBR 13.3+).

## 3. Auth
```bash
databricks auth login --host https://<workspace-host>
```

## 4. Run
Use `scripts/orchestrate_medallion.py` with the `job_id` from step 2.

## Unattended (Cowork) note
For scheduled runs, wrap step 3 (discovery) + the orchestrator call in a single
task. Daily/intraday scheduling is supported. Keep the discovery JSON and run
output under the project's operational state folder for audit.
