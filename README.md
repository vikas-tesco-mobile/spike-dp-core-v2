# spike-dp-core-v2 (Approach 2 - Centralized Bundle)

## Approach

This repo is the single deployment authority. It owns all workflows (bronze, silver, and dbt), the Spark source code, configs, and the only Databricks Asset Bundle.

The analysts repo, `spike-dp-analysts-v2`, remains a pure dbt code repository with no bundle and no standalone CD. In the current tactical `v2` direction, analysts CI uploads `dbt_project/` to a dedicated Databricks workspace folder outside the bundle-managed path, then deploys this repo. The dbt jobs are intended to run from that external workspace folder rather than Databricks `git_source` or a bundle-staged copy.

## What This Repo Contains

| Component | Description |
|---|---|
| `src/dp_core/` | Spark Python code: extractors, loaders, transformers, exporters, housekeeping, utilities |
| `configs/envs/` | Jinja2 config template (`config.yml.j2`) and `.env.template` |
| `configs/schemas/` | Silver layer schema definitions |
| `workflows/` | All Databricks workflow YAMLs, including dbt jobs |
| `tests/` | Python unit tests (`pytest`) |
| `databricks.yml` | Databricks Asset Bundle config for workflows, configs, and dbt task runtime settings |
| `pyproject.toml` | Python project config (`dp_core`) |

## Workflows Owned

### Finance
- `tesco_locations_api_daily` -- extract API -> bronze -> silver
- `tesco_products_api_daily` -- extract API -> bronze -> silver
- `tesco_quote_stream_daily` -- bronze only
- `tesco_tap_store_online_sales` -- bronze -> silver
- `sale_transactions_exporter` -- gold -> CSV export
- `tesco_sales_transactions` -- dbt job intended to run from an external Databricks workspace dbt folder

### CCS / Revenue Assurance
- `vmo2_mft_usage_daily` -- bronze -> silver
- `offer_manager_daily` -- bronze
- `hansen_daily` -- bronze
- `blugem_ccs_usage_reconciliation_exporter` -- gold -> CSV export
- `revenue_assurance_dbt` -- dbt job intended to run from an external Databricks workspace dbt folder

### Housekeeping
- `table_metadata_updater` -- metadata tags and descriptions

## How dbt Workflows Use Analysts dbt Code

dbt workflows are being moved to run from a dedicated Databricks workspace folder outside the bundle-managed path. Analysts CI uploads `spike-dp-analysts-v2/dbt_project` to that fixed workspace location before deploying this repo.

Example shape:

```yaml
tasks:
  - dbt_task:
      source: WORKSPACE
      project_directory: /Workspace/Shared/dbt-projects/spike-dp-analysts-v2/<env>/dbt_project
```

### Why this was introduced

This change was introduced because bundle deployment from `spike-dp-core-v2` is authoritative over the bundle-managed workspace path. When `dbt_project/` was staged inside the core bundle payload, CI/CD deploys could overwrite or remove the workspace copy if staging failed or if the copied files were not treated as part of the bundle source set. Moving the dbt project to a workspace folder outside DAB control avoids that overwrite behaviour.

## CI/CD

| Pipeline | What It Does |
|---|---|
| `ci.yml` | Snyk scan, pre-commit (ruff, mypy), pytest with coverage |
| `cd.yml` | Builds the wheel and deploys all workflows via `databricks bundle deploy` |

## Cross-Repo Deployment Flow

```text
Analyst merges dbt change to main (spike-dp-analysts-v2)
        |
        v
Analysts CI runs lint + dbt unit tests
        |
        v
Analysts CI uploads dbt_project/ to a dedicated Databricks workspace folder
        |
        v
Runs databricks bundle deploy from spike-dp-core-v2
        |
        v
dbt workflows run from the external workspace dbt_project folder
```

Required secret in analysts repo:
- `CORE_REPO_DISPATCH_TOKEN` -- PAT with read access to this private repo

## Current Limitations

- Analysts repo is not independently deployable. Analysts can develop and unit-test models locally, but cannot validate deployed runtime behaviour from their own repo because deployment is controlled here.
- Breaking changes are hard for analysts to detect. If bundle structure, workflow configuration, sync rules, or runtime paths change in core, analysts may be affected without visibility or control in their own repo.
- Local deployment from `spike-dp-core-v2` is incomplete by default. The dbt project lives in a separate repo and is expected to be uploaded to a separate Databricks workspace folder first.
- Successful deployment does not guarantee a working dbt runtime. The bundle can deploy cleanly while dbt jobs fail at runtime if the staged dbt project is missing, empty, or incorrectly copied.
- Core bundle deploy and dbt project upload are now separate lifecycle steps. This avoids DAB overwriting the dbt project, but makes deployment less atomic.
- Ownership becomes ambiguous. It is unclear whether the deployable source of truth for dbt lives in the analysts repo or in the staged copy inside core.
- Neither repo is self-sufficient. Analysts cannot test deployment behaviour end-to-end, and core cannot test dbt workflows without pulling content from another repo.
- CI staging adds its own fragility. Replacing `git_source` with workspace-synced files removes the Databricks Git credential dependency, but introduces a new risk: the staged copy can drift from the source repo if the CI pipeline is not carefully maintained.

## Assessment

`v2` achieves centralized deployment at the cost of clarity, local autonomy, and runtime simplicity. The model works, but it is harder to reason about and more fragile than either a clean split-bundle approach (`v1`) or an explicit versioned-artifact approach (`v3`).

## Local Development

```bash
poetry install --with dev
cp configs/envs/.env.template configs/envs/.env
# Edit .env with your DEVELOPER_PREFIX

# To test dbt workflows locally, first upload dbt_project/ from spike-dp-analysts-v2
# to the configured Databricks workspace folder
databricks bundle deploy --target local
```

## Relationship With spike-dp-analysts-v2

```text
spike-dp-core-v2                       spike-dp-analysts-v2
(this repo)                            (separate repo)
ALL workflows                          dbt models only (no bundle)
Spark code + wheel                     CI: lint + dbt unit tests
configs + schemas                      On merge to main:
databricks.yml (only bundle)             checks out this repo,
         |                                uploads dbt_project/
         |                                to workspace, then deploys from core
         v
dbt workflows use an external
workspace dbt_project folder
```
