# spike-dp-core-v2 (Approach 2 - Centralized Bundle)

## Approach

This repo is the single deployment authority -- it owns ALL workflows (bronze, silver, AND dbt), the Spark source code, configs, and the only Databricks Asset Bundle. In this tactical v2 variant, the analysts repo remains the source of truth for dbt code, but its `dbt_project/` is staged into this repo's bundle before deployment. The analysts repo has no bundle and no CD of its own. When analysts merge dbt changes to main, their CI triggers this repo's CD via `repository_dispatch`, which rebuilds and redeploys all workflows with the latest staged dbt code.

## What This Repo Contains

| Component | Description |
|---|---|
| `src/dp_core/` | Spark Python code -- extractors, loaders, transformers, exporters, housekeeping, utilities |
| `configs/envs/` | Jinja2 config template (`config.yml.j2`) and `.env.template` |
| `configs/schemas/` | Silver layer schema definitions (12 YAML files) |
| `dbt_project/` | Staged dbt project copied from analysts repo for deployment and validation |
| `workflows/` | **12 Databricks workflow YAMLs** (ALL workflows including dbt) |
| `tests/` | Python unit tests (pytest) |
| `databricks.yml` | Databricks Asset Bundle config (bundle name: `dp-core-v2`) that syncs configs and `dbt_project/` into workspace files |
| `pyproject.toml` | Python project config (package name: `dp-core`, internal: `dp_core`) |

## Workflows Owned

### Finance
- `tesco_locations_api_daily` -- extract API -> bronze -> silver
- `tesco_products_api_daily` -- extract API -> bronze -> silver
- `tesco_quote_stream_daily` -- bronze only
- `tesco_tap_store_online_sales` -- bronze -> silver (store + online)
- `sale_transactions_exporter` -- gold -> CSV export
- `tesco_sales_transactions` -- **dbt job** (runs staged `dbt_project/` from workspace files)

### CCS / Revenue Assurance
- `vmo2_mft_usage_daily` -- bronze -> 8 silver tables
- `offer_manager_daily` -- 3 bronze tables
- `hansen_daily` -- 5 bronze tables
- `blugem_ccs_usage_reconciliation_exporter` -- gold -> CSV export
- `revenue_assurance_dbt` -- **dbt job** (runs staged `dbt_project/` from workspace files)

### Housekeeping
- `table_metadata_updater` -- metadata tags and descriptions

## How dbt Workflows Use Analysts dbt Code

In this tactical v2 variant, analysts CI stages `dbt_project/` into this repo before deployment, and
Databricks dbt tasks run from workspace files:

```yaml
tasks:
  - dbt_task:
      source: WORKSPACE
      project_directory: ${workspace.file_path}/dbt_project
```

## CI/CD

| Pipeline | What It Does |
|---|---|
| `ci.yml` | Snyk vulnerability scan, pre-commit (ruff, mypy), pytest with coverage |
| `cd.yml` | Builds wheel, deploys ALL workflows to dev via `databricks bundle deploy`. Also triggered by `repository_dispatch` from analysts repo |

## Cross-Repo Deployment Flow

```
Analyst merges dbt change to main (spike-dp-analysts-v2)
        |
        v
Analysts CI runs (lint + dbt unit tests)
        |
        v  (on success, main branch only)
trigger-core-deploy job --> repository_dispatch "dbt-updated"
        |
        v
This repo's CD receives event
        |
        v
Stage analysts dbt_project/ --> build wheel --> databricks bundle deploy
(dbt workflows run from staged workspace files)
```

**Required secret in analysts repo:** `CORE_REPO_DISPATCH_TOKEN` (PAT with `repo` scope for this repo)

## Local Development

```bash
poetry install --with dev
cp configs/envs/.env.template configs/envs/.env
# Edit .env with your DEVELOPER_PREFIX
# Ensure dbt_project/ is present before bundle validation or deployment
databricks bundle deploy --target local
```

## Relationship With spike-dp-analysts-v2

```
spike-dp-core-v2                       spike-dp-analysts-v2
(this repo)                            (separate repo)
 ALL workflows                          dbt models only (no bundle)
 Spark code + wheel                     CI: lint + dbt unit tests
 configs + schemas                      On merge to main:
 dbt_project staged here                  triggers this repo's CD
 databricks.yml (only bundle)             via repository_dispatch
         |                                via repository_dispatch
         v
 dbt workflows run staged
 dbt_project from workspace
```
