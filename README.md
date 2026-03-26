# S3 Migration Orchestrator

Python 3 orchestration script for migrating objects from:

- Source: `s3://bh-pl-prod-static` (`me-south-1`)
- Destination: `s3://bh-pl-prod-static-mumbai` (`ap-south-1`)

The script uses AWS CLI for transfer operations (`aws s3 sync`, `aws s3 cp`) and Python for discovery, worker control, state, logging, and retry workflows.

## What It Migrates

- `fonts/`
- `404.html`
- All top-level folders under `upload/` discovered dynamically from source bucket
- Skips only `upload/_tmp`

## Architecture (Short)

- `s3_migration.py run`:
  - Discovers upload folders via `aws s3 ls s3://.../upload/`
  - Runs `fonts/` and `404.html` first
  - Runs upload folder syncs with controlled concurrency (`--max-parallel`, default `5`)
  - Tracks PID, start/end time, duration, exit code, and final status per job
  - Writes:
    - master logs/state
    - per-job logs/state
    - failed-key retry files for upload folders

- `s3_migration.py retry`:
  - Reads failed keys from a failed-keys file
  - Retries each key with `aws s3 cp`
  - Writes retry raw/summary logs

## Status Model

Jobs move through:

- `QUEUED`
- `STARTING`
- `RUNNING`
- `SUCCESS`
- `PARTIAL`
- `FAILED`
- `INTERRUPTED`

Notes:

- A process disappearing from `ps`/`grep` may mean it exited normally.
- `aws s3 sync` can be partially successful (some objects copied, some failed).
- In quiet mode (`--only-show-errors`), successful `copy:` lines are hidden, so `copied_objects` may be `unknown`.

## File Layout (`~/s3-migration`)

Master/global:

- `master_parallel.raw.log`
- `master_parallel.summary.log`
- `master_parallel.errors.log`
- `master_parallel.state.json`

Per upload folder:

- `<sanitized-folder>_parallel.raw.log`
- `<sanitized-folder>_parallel.summary.log`
- `<sanitized-folder>_parallel.state.json`

Failed keys:

- `failed-keys/upload_<sanitized-folder>.txt`

Retry logs:

- `retry_<sanitized-folder>.raw.log`
- `retry_<sanitized-folder>.summary.log`

## Run Commands

```bash
cd /home/andrew/Desktop/Platinumlist/s3-sync
python3 s3_migration.py run
```

```bash
python3 s3_migration.py run --max-parallel 5
```

```bash
python3 s3_migration.py run --max-parallel 5 --daemon
```

```bash
python3 s3_migration.py run --max-parallel 5 --verbose-copy-stats
```

Optional safe resume behavior:

```bash
python3 s3_migration.py run --skip-successful
```

## Retry Commands

Retry by folder name:

```bash
python3 s3_migration.py retry --folder dtcm-invoice
```

Retry by explicit failed-file path:

```bash
python3 s3_migration.py retry --failed-file ~/s3-migration/failed-keys/upload_dtcm-invoice.txt
```

## Tail Commands

Master summary log:

```bash
tail -f ~/s3-migration/master_parallel.summary.log
```

Master error log:

```bash
tail -f ~/s3-migration/master_parallel.errors.log
```

Per-folder summary log (example `dtcm-invoice`):

```bash
tail -f ~/s3-migration/dtcm-invoice_parallel.summary.log
```

Per-folder raw log (example `dtcm-invoice`):

```bash
tail -f ~/s3-migration/dtcm-invoice_parallel.raw.log
```

## Inspect Commands

List failed-key files:

```bash
ls -lah ~/s3-migration/failed-keys/
```

View one failed-key file:

```bash
sed -n '1,200p' ~/s3-migration/failed-keys/upload_dtcm-invoice.txt
```

Show active AWS CLI transfer processes:

```bash
ps -eo pid,etimes,cmd | rg "aws s3 (sync|cp)"
```

Show current master state JSON:

```bash
python3 -m json.tool ~/s3-migration/master_parallel.state.json | sed -n '1,200p'
```

## CLI Help

```bash
python3 s3_migration.py --help
python3 s3_migration.py run --help
python3 s3_migration.py retry --help
```
# s3-cross-region-custom-sync
