#!/usr/bin/env python3
"""Generate S3 Batch Operations manifest CSV from S3 object listing."""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

SOURCE_BUCKET = "bh-pl-prod-static"
SOURCE_REGION = "me-south-1"
DEFAULT_RUNTIME_DIR = Path("runtime") / "generate_manifest"

PROGRESS_EVERY = 10_000
FLUSH_EVERY = 10_000


def should_include(key: str) -> bool:
    """Return True when key should be included in manifest output."""
    if key.startswith("dist/"):
        return False
    if key.startswith("upload/_tmp/"):
        return False
    return True


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _build_paths(runtime_dir: Path) -> Dict[str, Path]:
    return {
        "runtime_dir": runtime_dir,
        "manifest_final": runtime_dir / "manifest.csv",
        "manifest_temp": runtime_dir / "manifest.csv.tmp",
        "app_log": runtime_dir / "generate_manifest.log",
        "stdout_log": runtime_dir / "manifest.stdout.log",
        "master_log": runtime_dir / "manifest_master.log",
        "master_state": runtime_dir / "manifest_master.state.json",
    }


def _append_master_log(paths: Dict[str, Path], message: str) -> None:
    with paths["master_log"].open("a", encoding="utf-8") as fh:
        fh.write(f"[{_ts()}] {message}\n")


def _write_master_state(paths: Dict[str, Path], payload: Dict[str, Any]) -> None:
    paths["master_state"].write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def setup_logging(paths: Dict[str, Path]) -> logging.Logger:
    """Configure logger for both stdout and file with timestamps."""
    logger = logging.getLogger("generate_manifest")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(paths["app_log"], encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


def generate_manifest(logger: logging.Logger, paths: Dict[str, Path]) -> int:
    """Scan source bucket with paginator and write manifest incrementally."""
    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as exc:
        logger.exception("Missing dependency boto3/botocore: %s", exc)
        _append_master_log(paths, f"FAILED pid={os.getpid()} error=missing_boto3")
        _write_master_state(
            paths,
            {
                "pid": os.getpid(),
                "state": "failed",
                "started_at": _iso_now(),
                "finished_at": _iso_now(),
                "source_bucket": SOURCE_BUCKET,
                "source_region": SOURCE_REGION,
                "manifest_temp": str(paths["manifest_temp"].resolve()),
                "manifest_final": str(paths["manifest_final"].resolve()),
                "app_log": str(paths["app_log"].resolve()),
                "stdout_log": str(paths["stdout_log"].resolve()),
                "master_log": str(paths["master_log"].resolve()),
                "counters": {"scanned": 0, "included": 0, "excluded": 0},
                "error": f"missing dependency: {exc}",
            },
        )
        return 1

    session = boto3.session.Session(region_name=SOURCE_REGION)
    s3_client = session.client("s3", region_name=SOURCE_REGION)
    paginator = s3_client.get_paginator("list_objects_v2")

    total_scanned = 0
    total_included = 0
    total_excluded = 0

    logger.info("Manifest generation started")
    logger.info("Source bucket=%s source_region=%s", SOURCE_BUCKET, SOURCE_REGION)
    logger.info(
        "Output temp file=%s final file=%s",
        paths["manifest_temp"],
        paths["manifest_final"],
    )
    logger.info("Filter: exclude dist/ and upload/_tmp/, include everything else")

    paths["runtime_dir"].mkdir(parents=True, exist_ok=True)
    pid = os.getpid()
    run_started_at = _iso_now()
    _append_master_log(paths, f"RUNNING pid={pid} started_at={run_started_at}")
    _write_master_state(
        paths,
        {
            "pid": pid,
            "state": "running",
            "started_at": run_started_at,
            "finished_at": None,
            "source_bucket": SOURCE_BUCKET,
            "source_region": SOURCE_REGION,
            "manifest_temp": str(paths["manifest_temp"].resolve()),
            "manifest_final": str(paths["manifest_final"].resolve()),
            "app_log": str(paths["app_log"].resolve()),
            "stdout_log": str(paths["stdout_log"].resolve()),
            "master_log": str(paths["master_log"].resolve()),
            "counters": {"scanned": 0, "included": 0, "excluded": 0},
            "error": None,
        },
    )

    try:
        with paths["manifest_temp"].open("w", encoding="utf-8", newline="") as manifest_fh:
            pages = paginator.paginate(Bucket=SOURCE_BUCKET)
            for page in pages:
                contents = page.get("Contents", [])
                for obj in contents:
                    key = obj["Key"]
                    total_scanned += 1

                    if should_include(key):
                        manifest_fh.write(f"{SOURCE_BUCKET},{key}\n")
                        total_included += 1
                    else:
                        total_excluded += 1

                    if total_scanned % PROGRESS_EVERY == 0:
                        logger.info(
                            "Progress scanned=%d included=%d excluded=%d",
                            total_scanned,
                            total_included,
                            total_excluded,
                        )
                        _append_master_log(
                            paths,
                            (
                                f"PROGRESS pid={pid} scanned={total_scanned} "
                                f"included={total_included} excluded={total_excluded}"
                            ),
                        )
                        _write_master_state(
                            paths,
                            {
                                "pid": pid,
                                "state": "running",
                                "started_at": run_started_at,
                                "finished_at": None,
                                "source_bucket": SOURCE_BUCKET,
                                "source_region": SOURCE_REGION,
                                "manifest_temp": str(paths["manifest_temp"].resolve()),
                                "manifest_final": str(paths["manifest_final"].resolve()),
                                "app_log": str(paths["app_log"].resolve()),
                                "stdout_log": str(paths["stdout_log"].resolve()),
                                "master_log": str(paths["master_log"].resolve()),
                                "counters": {
                                    "scanned": total_scanned,
                                    "included": total_included,
                                    "excluded": total_excluded,
                                },
                                "error": None,
                            },
                        )
                    if total_scanned % FLUSH_EVERY == 0:
                        manifest_fh.flush()
                        os.fsync(manifest_fh.fileno())

            manifest_fh.flush()
            os.fsync(manifest_fh.fileno())

        paths["manifest_temp"].replace(paths["manifest_final"])

        logger.info("Manifest generation completed successfully")
        logger.info("Total scanned objects=%d", total_scanned)
        logger.info("Total included objects=%d", total_included)
        logger.info("Total excluded objects=%d", total_excluded)
        logger.info("Output file path=%s", paths["manifest_final"].resolve())
        finished_at = _iso_now()
        _append_master_log(
            paths,
            (
                f"DONE pid={pid} finished_at={finished_at} scanned={total_scanned} "
                f"included={total_included} excluded={total_excluded}"
            ),
        )
        _write_master_state(
            paths,
            {
                "pid": pid,
                "state": "done",
                "started_at": run_started_at,
                "finished_at": finished_at,
                "source_bucket": SOURCE_BUCKET,
                "source_region": SOURCE_REGION,
                "manifest_temp": str(paths["manifest_temp"].resolve()),
                "manifest_final": str(paths["manifest_final"].resolve()),
                "app_log": str(paths["app_log"].resolve()),
                "stdout_log": str(paths["stdout_log"].resolve()),
                "master_log": str(paths["master_log"].resolve()),
                "counters": {
                    "scanned": total_scanned,
                    "included": total_included,
                    "excluded": total_excluded,
                },
                "error": None,
            },
        )
        return 0

    except (ClientError, BotoCoreError) as exc:
        logger.exception("AWS error while generating manifest: %s", exc)
        finished_at = _iso_now()
        _append_master_log(paths, f"FAILED pid={pid} finished_at={finished_at} error={exc}")
        _write_master_state(
            paths,
            {
                "pid": pid,
                "state": "failed",
                "started_at": run_started_at,
                "finished_at": finished_at,
                "source_bucket": SOURCE_BUCKET,
                "source_region": SOURCE_REGION,
                "manifest_temp": str(paths["manifest_temp"].resolve()),
                "manifest_final": str(paths["manifest_final"].resolve()),
                "app_log": str(paths["app_log"].resolve()),
                "stdout_log": str(paths["stdout_log"].resolve()),
                "master_log": str(paths["master_log"].resolve()),
                "counters": {
                    "scanned": total_scanned,
                    "included": total_included,
                    "excluded": total_excluded,
                },
                "error": str(exc),
            },
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unexpected error while generating manifest: %s", exc)
        finished_at = _iso_now()
        _append_master_log(paths, f"FAILED pid={pid} finished_at={finished_at} error={exc}")
        _write_master_state(
            paths,
            {
                "pid": pid,
                "state": "failed",
                "started_at": run_started_at,
                "finished_at": finished_at,
                "source_bucket": SOURCE_BUCKET,
                "source_region": SOURCE_REGION,
                "manifest_temp": str(paths["manifest_temp"].resolve()),
                "manifest_final": str(paths["manifest_final"].resolve()),
                "app_log": str(paths["app_log"].resolve()),
                "stdout_log": str(paths["stdout_log"].resolve()),
                "master_log": str(paths["master_log"].resolve()),
                "counters": {
                    "scanned": total_scanned,
                    "included": total_included,
                    "excluded": total_excluded,
                },
                "error": str(exc),
            },
        )
        return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate S3 Batch manifest CSV")
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Run in background mode and return immediately",
    )
    parser.add_argument(
        "--runtime-dir",
        type=Path,
        default=DEFAULT_RUNTIME_DIR,
        help="Directory for local runtime artifacts (logs, state, tmp, manifest)",
    )
    args = parser.parse_args()
    runtime_dir = args.runtime_dir.expanduser()
    paths = _build_paths(runtime_dir)
    runtime_dir.mkdir(parents=True, exist_ok=True)

    if args.daemon and os.getenv("GENERATE_MANIFEST_DAEMON") != "1":
        stdout_log = paths["stdout_log"].open("a", encoding="utf-8")
        cmd = [sys.executable, str(Path(__file__).resolve()), "--runtime-dir", str(runtime_dir)]
        env = {**os.environ, "GENERATE_MANIFEST_DAEMON": "1"}
        proc = subprocess.Popen(
            cmd,
            stdout=stdout_log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=env,
        )
        stdout_log.close()
        _append_master_log(paths, f"DAEMON_STARTED daemon_pid={proc.pid}")
        _write_master_state(
            paths,
            {
                "daemon_pid": proc.pid,
                "state": "daemon_started",
                "started_at": _iso_now(),
                "finished_at": None,
                "runtime_dir": str(runtime_dir.resolve()),
                "manifest_final": str(paths["manifest_final"].resolve()),
                "manifest_temp": str(paths["manifest_temp"].resolve()),
                "stdout_log": str(paths["stdout_log"].resolve()),
                "app_log": str(paths["app_log"].resolve()),
                "master_log": str(paths["master_log"].resolve()),
            },
        )
        print(f"Manifest daemon started. PID={proc.pid}")
        print(f"Runtime dir: {runtime_dir.resolve()}")
        print(f"Stdout log: {paths['stdout_log'].resolve()}")
        print(f"App log: {paths['app_log'].resolve()}")
        print(f"Master log: {paths['master_log'].resolve()}")
        print(f"Master state: {paths['master_state'].resolve()}")
        return 0

    logger = setup_logging(paths)
    logger.info("Script start")
    return generate_manifest(logger, paths)


if __name__ == "__main__":
    sys.exit(main())
