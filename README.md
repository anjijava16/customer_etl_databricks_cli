# customer_etl — end-to-end Databricks Asset Bundle example

A minimal but production-shaped PySpark ETL: a 10-row customer CSV flows through
**bronze → silver → gold** Delta tables, packaged as a Python wheel and deployed
as a scheduled Databricks Job via **Databricks Asset Bundles (DABs)**.

```
CSV (data/customers.csv)
   → customers_bronze        raw rows + ingestion metadata
   → customers_silver        deduped, validated, normalized
   → spend_by_country_gold   aggregate for BI
```

## Repository layout

```
customer_etl/
├── databricks.yml                  # bundle: targets, variables, artifact build
├── pyproject.toml                  # wheel definition + `main` entry point
├── data/customers.csv              # sample input (10 records, incl. dirty rows)
├── src/customer_etl/
│   ├── main.py                     # entry point (argparse → bronze/silver/gold)
│   ├── ingestion.py                # CSV → bronze
│   ├── transform.py                # pure, unit-testable transforms
│   └── validation.py               # fail-fast quality checks
├── resources/customer_etl_job.yml  # the Databricks Job (serverless wheel task)
├── tests/test_transform.py         # pytest against a local SparkSession
└── .github/workflows/deploy.yml    # CI: test + validate on PR, deploy on main
```

## Prerequisites

- New Databricks CLI (v0.2xx+): `brew tap databricks/tap && brew install databricks`
- Python 3.10+ with `pip install build`
- Authenticated to your workspace:

```bash
databricks auth login --host https://dbc-d40d7b72-eeb2.cloud.databricks.com
databricks current-user me   # sanity check
```

## Run it (4 commands)

```bash
cd customer_etl

pip install build                       # needed once, to build the wheel
databricks bundle validate -t dev       # 1. check config
databricks bundle deploy   -t dev       # 2. build wheel, upload code+data, create job
databricks bundle run customer_etl_job -t dev   # 3. trigger the job, stream status
```

Then in the workspace UI: **Jobs & Pipelines** → `[dev yourname] customer-etl-medallion`
to see the run, logs, and task output. Query results:

```sql
SELECT * FROM workspace.customer_etl.spend_by_country_gold;
```

Expected: 10 raw rows → 8 silver rows (1 duplicate + 1 negative-spend row dropped)
→ 7 gold rows (one per country).

## Configuration notes

- **Catalog/schema** default to `workspace.customer_etl` (Free Edition default).
  Override at deploy time: `databricks bundle deploy -t dev --var="catalog=main"`.
- **Compute** is serverless (`environment_key`), which works on Free Edition and
  trial workspaces. A classic job-cluster alternative is commented in
  `resources/customer_etl_job.yml`.
- **dev vs prod targets**: `dev` (development mode) prefixes everything with your
  username and pauses schedules — safe to redeploy constantly. `prod`
  (production mode) deploys to a shared path and honors the schedule.

## Local unit tests

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

The transforms are pure `DataFrame → DataFrame` functions, so they run on a local
SparkSession with no Databricks connection.

## Push to GitHub + CI/CD

```bash
git init && git add . && git commit -m "customer_etl DAB example"
git remote add origin git@github.com:anjijava16/customer_etl.git
git push -u origin main
```

For the Actions workflow, add repo secrets:

| Secret | Value |
|---|---|
| `DATABRICKS_HOST` | `https://dbc-d40d7b72-eeb2.cloud.databricks.com` |
| `DATABRICKS_CLIENT_ID` | Service principal application ID |
| `DATABRICKS_CLIENT_SECRET` | Service principal OAuth secret |

(Or for a quick personal setup, replace client id/secret with `DATABRICKS_TOKEN`
using a PAT.) PRs run tests + `bundle validate`; merges to `main` run
`bundle deploy -t prod`.

## Clean up

```bash
databricks bundle destroy -t dev
```
