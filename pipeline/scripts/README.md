# Pipeline Utility Scripts

This directory contains utility and operations scripts for the Data Commons import and ingestion pipeline.

---

## Available Scripts

| Script | Description |
| :--- | :--- |
| **[`run_ingestion.sh`](run_ingestion.sh)** | Triggers the Cloud Spanner ingestion workflow for an import via `ingestion-helper` (supports `--dry-run`). |
| **[`update_lock.sh`](update_lock.sh)** | Releases or force-acquires the global Spanner ingestion lock for a workflow execution. |
| **[`delete_provenance.py`](delete_provenance.py)** | Deletes Observation rows, then TimeSeries rows, for one provenance. Supports a preview and an optional statvar filter. |

---

## `run_ingestion.sh`

Triggers the `spanner-ingestion-workflow` for an import through the `ingestion-helper` service (`POST /imports/ingest`). The import is sent with `forceIngestion: true`, so it is ingested even if that version already succeeded. The script does not update the import's version in Spanner.

### Syntax

```bash
./pipeline/scripts/run_ingestion.sh <importName> <env> <latestVersion> [--dry-run]
```

### Arguments

- **`importName`** *(required)*: The full import name including script path prefix (e.g. `scripts/us_fed/treasury_constant_maturity_rates:USFed_ConstantMaturityRates_Test`). Only the part after `:` is sent.
- **`env`** *(required)*: Target environment (`staging` or `prod`).
- **`latestVersion`** *(required)*: Full GCS path (with wildcard) to the graph files to ingest (e.g. `'gs://datcom-prod-imports/scripts/us_fed/treasury_constant_maturity_rates/USFed_ConstantMaturityRates_Test/2025_12_17T02_30_27_233484_08_00/**/*.mcf*'`).
- **`--dry-run`** *(optional)*: The helper resolves the import list and returns it without starting the workflow (`status: SKIPPED`).

### Example

```bash
./pipeline/scripts/run_ingestion.sh \
  scripts/us_fed/treasury_constant_maturity_rates:USFed_ConstantMaturityRates_Test \
  staging \
  'gs://datcom-prod-imports/scripts/us_fed/treasury_constant_maturity_rates/USFed_ConstantMaturityRates_Test/2025_12_17T02_30_27_233484_08_00/**/*.mcf*' \
  --dry-run
```

On success without `--dry-run`, the response has `status: SUBMITTED` and the workflow `executionName`.

---

## `update_lock.sh`

Updates the global Spanner ingestion lock (`IngestionLock`) through the `ingestion-helper` service.

### Syntax

```bash
./pipeline/scripts/update_lock.sh <release|acquire> <workflowId> <env>
```

### Arguments

- **`mode`** *(required)*:
  - `release`: Releases the lock held by `workflowId` (`POST /database/lock/release`). Fails if the lock is not held by that workflow.
  - `acquire`: Force-assigns the lock to `workflowId`, even if another workflow holds it (`POST /database/lock/acquire` with `force: true`).
- **`workflowId`** *(required)*: The Cloud Workflows execution ID.
- **`env`** *(required)*: Target environment (`staging` or `prod`).

### When to use

- A cancelled or crashed workflow never reaches its release step, so it keeps the lock. Use `release` to free it.
- Waiting workflows poll for the lock in no particular order, so after a `release` any of them may grab it. To hand the lock to a specific execution (e.g. a rerun after a failure), use `acquire` with that execution's ID instead; it picks up the lock on its next poll.

> [!CAUTION]
> `acquire` takes the lock away from whichever workflow holds it, including a running one. Make sure the current owner is no longer running first.

### Examples

```bash
# Free the lock held by a cancelled execution.
./pipeline/scripts/update_lock.sh release 12345678-1234-1234-1234-123456789abc staging

# Hand the lock to a waiting rerun execution.
./pipeline/scripts/update_lock.sh acquire 12345678-1234-1234-1234-123456789abc prod
```

## `delete_provenance.py`

Use `delete_provenance.py` to delete Observation and TimeSeries rows for one
provenance in Spanner. It leaves Edge, Node, and other tables unchanged.
You can delete all statvars for that provenance or select specific statvars
with `--sv`.

The script deletes Observation rows before TimeSeries rows and preserves
change-stream settings so CDC can still record the deletes.

### Before you start

Use Python 3.11 or later and Application Default Credentials for the target
database. `uv run` installs `google-cloud-spanner` for the script. You can also
run the script with Python if that package is already installed.

Run the commands below from the `import` repository root. To see all options:

```bash
uv run pipeline/scripts/delete_provenance.py --help
```

### Preview the deletes

Replace the example project, instance, database, and provenance with your
values. Use the exact provenance stored in Spanner, including `dc/base/`
if present.

Without `--sv`, the script selects all statvars for the provenance. To select
one statvar, add `--sv STATVAR_DCID`. Repeat `--sv` to select more than one.

Run the preview. It reads Spanner but does not delete data:

```bash
uv run pipeline/scripts/delete_provenance.py \
	--project datcom-store \
	--instance dc-graph-staging \
	--database dc_graph \
	--provenance dc/base/EPACriteriaAirPollutants
```

The logs list the selected statvars and planned delete statements. The
preview does not count Observation rows.

### Check the scope and delete

Before you execute the deletes:

1. Check the target database, provenance, and selected statvars in the preview
   logs.
2. Confirm that each selected `(variable_measured, facet_id)` pair belongs
   only to the target provenance. The script does not check ownership.
3. Stop ingestion, rollback, and other jobs that could write these rows.
   Keep those jobs stopped until deletion finishes, including during recovery
   after a failure. The script does not acquire a lock.
4. Run the same command with `--execute` to delete the selected rows.

Check the logs for TimeSeries completion, or a message that no matching rows
were found. Each completed statement reports its duration and a deleted-row
count of "at least N rows". Partitioned DML does not return an exact row count.

### Recover from a failure

If a statement fails, the script stops with an error. It does not undo deletes
that have already committed. Resolve the error, then rerun the same command
with the same provenance and statvar selection to finish deletion.
The script keeps TimeSeries rows until all Observation deletes succeed.

### Run the tests

Run this command without connecting to Spanner:

```bash
uv run --with google-cloud-spanner python -m unittest discover \
	-s pipeline/scripts -p delete_provenance_test.py
```
