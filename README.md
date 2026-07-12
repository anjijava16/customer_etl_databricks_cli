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



# customer_etl — End-to-End Databricks Asset Bundle (Final, Verified)

A minimal but production-shaped PySpark ETL, deployed and **successfully run** on
Databricks using **Databricks Asset Bundles (DABs)**. A 10-row customer CSV flows
through **bronze → silver → gold** Delta tables, packaged as a Python wheel and
executed as a serverless Databricks Job.

This document covers the full journey: architecture, every command, **every issue
hit along the way** (with root cause and fix), and the verified final output.

```
CSV (data/customers.csv)
   → workspace.customer_etl.customers_bronze        raw rows + ingestion metadata
   → workspace.customer_etl.customers_silver        deduped, validated, normalized
   → workspace.customer_etl.spend_by_country_gold   aggregate for BI
```

**Verified result (run 324464902175107):**

```
[bronze] wrote 10 rows -> `workspace`.`customer_etl`.customers_bronze
[silver] wrote  8 rows -> `workspace`.`customer_etl`.customers_silver
[gold]   wrote  6 rows -> `workspace`.`customer_etl`.spend_by_country_gold
TERMINATED SUCCESS
```

Why 10 → 8 → 6: the raw file deliberately contains one **duplicate** row
(customer 1003) and one **negative-spend** row (customer 1006, MX). Silver drops
both (10 − 2 = 8). Since 1006 was the *only* MX customer, MX disappears entirely,
leaving 6 countries in gold (US, IN, GB, EG, PL, JP).

---

## 1. Architecture

```
Developer laptop
      │  python -m build  →  customer_etl-1.0.0-py3-none-any.whl
      ▼
databricks bundle deploy -t dev
      │  uploads wheel + bundle files, creates/updates the Job
      ▼
Databricks Workspace  (/Workspace/Users/<you>/.bundle/customer_etl/dev)
      │
      ▼
Job: [dev <you>] customer-etl-medallion   (serverless compute)
      │  installs wheel → runs entry point main()
      ▼
bronze → silver → gold Delta tables in Unity Catalog
```

Key design choices:

| Choice | Why |
|---|---|
| **Wheel task, not notebook** | Real package: imports, unit tests, versioning, rollback |
| **Serverless compute** | No cluster management; works on Free Edition/trial workspaces |
| **Explicit schema on read** | Never `inferSchema` in production — silent type drift |
| **Pure transform functions** | `DataFrame → DataFrame`, testable with local Spark, no Databricks needed |
| **Fail-fast validations** | Empty-layer and duplicate-key checks raise → task fails → alert fires |
| **`mode: development` target** | Resources prefixed `[dev <user>]`, schedules paused — safe to redeploy |

## 2. Repository layout

```
customer_etl/
├── databricks.yml                  # bundle: targets, variables, artifact build
├── pyproject.toml                  # wheel definition + `main` entry point
├── data/customers.csv              # sample input (10 records, incl. dirty rows)
├── src/customer_etl/
│   ├── main.py                     # entry point (argparse → bronze/silver/gold)
│   ├── ingestion.py                # CSV → bronze (+ _ingested_at, _source_file)
│   ├── transform.py                # pure, unit-testable transforms
│   └── validation.py               # fail-fast quality checks
├── resources/customer_etl_job.yml  # the Databricks Job (serverless wheel task)
├── tests/test_transform.py         # pytest against a local SparkSession
└── .github/workflows/deploy.yml    # CI: test + validate on PR, deploy on main
```

How the pieces connect at deploy time:

- `pyproject.toml` → `[project.scripts] main = "customer_etl.main:main"` defines
  the console entry point.
- `resources/customer_etl_job.yml` → `python_wheel_task: {package_name:
  customer_etl, entry_point: main}` calls that entry point on the cluster.
- Parameters are injected by the bundle:
  `--data-path file:${workspace.file_path}/data/customers.csv` resolves to the
  synced bundle files under `/Workspace/Users/<you>/.bundle/customer_etl/dev/files/`.
- `${var.catalog}` / `${var.schema}` default to `workspace` / `customer_etl` and
  can be overridden: `databricks bundle deploy -t dev --var="catalog=main"`.

## 3. Prerequisites

- **New Go-based Databricks CLI, v0.2xx+** (NOT the legacy pip `databricks-cli`):

  ```bash
  brew tap databricks/tap && brew install databricks
  databricks -v        # must show v0.2xx, not 0.17.x
  which databricks     # should be /opt/homebrew/bin or /usr/local/bin
  ```

- Python 3.10+ with the build tool: `pip install build`
- OAuth login to the workspace:

  ```bash
  databricks auth login --host https://dbc-d40d7b72-eeb2.cloud.databricks.com
  # when prompted "profile name", type:  dev
  databricks current-user me    # sanity check — must return your user JSON
  ```

## 4. Run it (the 4 commands)

```bash
cd customer_etl
pip install build                                # once — builds the wheel locally

databricks bundle validate -t dev                # 1. parse + check config against workspace
databricks bundle deploy   -t dev                # 2. build wheel, upload code+data, create job
databricks bundle run customer_etl_job -t dev    # 3. trigger the job, stream status
```

What `deploy` actually does: reads `databricks.yml` → runs `python3 -m build
--wheel` → uploads `dist/*.whl` and all bundle files → creates/updates the Job
via the API → records deployment state. No UI clicking.

Then verify in the workspace: **Jobs & Pipelines →** `[dev <you>]
customer-etl-medallion`, and in the SQL editor:

```sql
SELECT * FROM workspace.customer_etl.spend_by_country_gold ORDER BY total_spend DESC;
```

Expected gold output:

```
+-------+--------------+-----------+---------+
|country|customer_count|total_spend|avg_spend|
+-------+--------------+-----------+---------+
|US     |2             |3230.75    |1615.38  |
|IN     |2             |1700.75    |850.38   |
|GB     |1             |1580.3     |1580.3   |
|EG     |1             |1105.6     |1105.6   |
|PL     |1             |675.0      |675.0    |
|JP     |1             |320.45     |320.45   |
+-------+--------------+-----------+---------+
```

## 5. Local unit tests

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

The transforms are pure functions, so they run on `local[2]` Spark with no
Databricks connection — this is what CI runs on every PR.

---

## 6. Troubleshooting log — the real issues hit, in order

This section documents every failure encountered while getting to the first
successful run, because each one is a classic that almost every team hits.

### Issue 1 — `Error: No such command 'auth'` (CLI v0.17.8)

**Symptom:**

```
databricks auth login https://dbc-....cloud.databricks.com
Error: No such command 'auth'.
The version of the CLI you're using is v0.17.8.
```

**Root cause:** Two different products share the name `databricks`. The **legacy
Python CLI** (`pip install databricks-cli`, versions 0.17.x and below) has no
`auth` or `bundle` commands. It was sitting in the conda `base` environment and
shadowing everything.

**Fix:**

```bash
pip uninstall databricks-cli          # remove legacy CLI
brew tap databricks/tap && brew install databricks   # install new Go CLI
hash -r                                # reset shell command cache
which databricks && databricks -v      # confirm v0.2xx wins on PATH
```

**Lesson:** always check `databricks -v` first. Anything below v0.200 cannot do
bundles.

### Issue 2 — `403 Invalid access token` on `bundle validate`

**Symptom:**

```
Error: Invalid access token. (403)
Endpoint: GET https://dbc-d40d7b72-....cloud.databricks.com/api/2.0/preview/scim/v2/Me
```

**Root cause:** `~/.databrickscfg` contained stale profiles (`[DEFAULT]`,
`[dev]`, `[welcome]`) written by the legacy CLI, all with:

1. A personal access token (`dapi...`) for a **different workspace**
   (`dbc-725225c9...`), being sent to the new workspace (`dbc-d40d7b72...`).
2. A **malformed host** — a browser URL pasted verbatim:
   `https://dbc-....cloud.databricks.com/?autoLogin=true&account_id=...&o=...`
   Hosts in config must be the bare workspace URL, nothing after `.com`.

**Fix:** back up and rewrite the config to a single clean OAuth profile:

```ini
[__settings__]
auth_storage    = secure
default_profile = dev

[dev]
host         = https://dbc-d40d7b72-eeb2.cloud.databricks.com
account_id   = b7e5c6dc-bc2b-4b66-9a89-b79c19195e3f
workspace_id = 2262041767304032
auth_type    = databricks-cli      # OAuth via `databricks auth login`
```

Also: **revoke any `dapi...` token that leaked into backups or terminals**
(workspace → Settings → Developer → Access tokens).

**Bonus gotcha found here:** during an earlier `databricks auth login`, the text
`databricks auth profiles` was typed at the *"profile name"* prompt — creating a
profile literally named `databricks auth profiles`. Profile names are free text;
type a short name like `dev` at that prompt.

### Issue 3 — `has no DEFAULT profile configured ... Env: DATABRICKS_TOKEN`

**Symptom:**

```
Error: resolve: /Users/<you>/.databrickscfg has no DEFAULT profile configured.
Config: token=***, profile=DEFAULT. Env: DATABRICKS_TOKEN
```

**Root cause:** a leftover environment variable. The CLI's **credential
precedence** is:

```
1. Environment variables   (DATABRICKS_TOKEN, DATABRICKS_HOST, DATABRICKS_CONFIG_PROFILE)
2. --profile / -p flag or bundle `profile:` setting
3. Config file profile matching the target host
4. [DEFAULT] profile
```

`DATABRICKS_TOKEN=dapi...` was set in the shell, so the CLI ignored the valid
OAuth profile entirely and tried PAT auth with a dead token.

**Fix:**

```bash
env | grep DATABRICKS                 # diagnose — this is always step 1
unset DATABRICKS_TOKEN DATABRICKS_HOST DATABRICKS_CONFIG_PROFILE
# make it permanent — search shell startup files for exports:
grep -R "DATABRICKS" ~/.zshrc ~/.zprofile ~/.bash_profile ~/.bashrc 2>/dev/null
```

Remove/comment any `export DATABRICKS_...` lines found, then `source ~/.zshrc`
or open a new terminal. After this, `databricks bundle validate -t dev`
returned `Validation OK!` and deploy + run succeeded.

**Lesson:** when auth behaves inexplicably, `env | grep DATABRICKS` **before**
anything else — env vars silently outrank every profile.

### Quick diagnostic cheat-sheet

| Error | First thing to check |
|---|---|
| `No such command 'auth'/'bundle'` | `databricks -v` → legacy CLI on PATH |
| `403 Invalid access token` | Wrong/stale token or host mismatch in `~/.databrickscfg` |
| `Env: DATABRICKS_TOKEN` in error text | `env | grep DATABRICKS` → unset leftovers |
| Host has `?autoLogin=...` in config | Trim to bare `https://dbc-....cloud.databricks.com` |
| `cannot resolve bundle auth configuration` | Add `profile:` under target's `workspace:` or use `-p` |

---

## 7. CI/CD (GitHub Actions)

Push to GitHub:

```bash
git init && git add . && git commit -m "customer_etl DAB example, verified E2E"
git remote add origin git@github.com:anjijava16/customer_etl.git
git push -u origin main
```

Repo secrets for `.github/workflows/deploy.yml`:

| Secret | Value |
|---|---|
| `DATABRICKS_HOST` | `https://dbc-d40d7b72-eeb2.cloud.databricks.com` |
| `DATABRICKS_CLIENT_ID` | Service principal application ID |
| `DATABRICKS_CLIENT_SECRET` | Service principal OAuth secret |

Behavior: PRs → `pytest` + `bundle validate`; merge to `main` → `bundle deploy
-t prod`. Note the deliberate irony: CI uses those same env vars
(`DATABRICKS_CLIENT_ID/SECRET`) that caused Issue 3 locally — env-var precedence
is a *feature* in CI and a *trap* on laptops.

## 8. Cleanup / redeploy

```bash
databricks bundle destroy -t dev     # removes the job + bundle files
databricks bundle deploy  -t dev     # idempotent — safe to run repeatedly
```

## 9. Next steps

- Add a second task: a **Lakeflow Declarative Pipeline (DLT)** reading the same
  CSV, so the repo demos both wheel jobs and declarative pipelines.
- Switch trigger to **file arrival** on a Volume path for near-realtime.
- Split `qa`/`prod` targets with `--var="catalog=..."` per environment.
- Wire failure alerts to Slack/PagerDuty via job `webhook_notifications`.