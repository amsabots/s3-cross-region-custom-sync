#!/usr/bin/env python3
"""Generate S3 Batch Operations manifest CSV from S3 object listing."""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError


SOURCE_BUCKET = "bh-pl-prod-static"
SOURCE_REGION = "me-south-1"

MANIFEST_FINAL_PATH = Path("manifest.csv")
MANIFEST_TEMP_PATH = Path("manifest.csv.tmp")
LOG_PATH = Path("generate_manifest.log")
STDOUT_LOG_PATH = Path("manifest.stdout.log")
STATE_PATH = Path("generate_manifest.state.json")

PROGRESS_EVERY = 10_000
FLUSH_EVERY = 10_000


def should_include(key: str) -> bool:
    """Return True when key should be included in manifest output."""
    if key.startswith("dist/"):
        return False
    if key.startswith("upload/_tmp/"):
        return False
    return True


def setup_logging() -> logging.Logger:
    """Configure logger for both stdout and file with timestamps."""
    logger = logging.getLogger("generate_manifest")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


def generate_manifest(logger: logging.Logger) -> int:
    """Scan source bucket with paginator and write manifest incrementally."""
    session = boto3.session.Session(region_name=SOURCE_REGION)
    s3_client = session.client("s3", region_name=SOURCE_REGION)
    paginator = s3_client.get_paginator("list_objects_v2")

    total_scanned = 0
    total_included = 0
    total_excluded = 0

    logger.info("Manifest generation started")
    logger.info("Source bucket=%s source_region=%s", SOURCE_BUCKET, SOURCE_REGION)
    logger.info("Output temp file=%s final file=%s", MANIFEST_TEMP_PATH, MANIFEST_FINAL_PATH)
    logger.info("Filter: exclude dist/ and upload/_tmp/, include everything else")

    MANIFEST_TEMP_PATH.parent.mkdir(parents=True, exist_ok=True)

    try:
        with MANIFEST_TEMP_PATH.open("w", encoding="utf-8", newline="") as manifest_fh:
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
                    if total_scanned % FLUSH_EVERY == 0:
                        manifest_fh.flush()
                        os.fsync(manifest_fh.fileno())

            manifest_fh.flush()
            os.fsync(manifest_fh.fileno())

        MANIFEST_TEMP_PATH.replace(MANIFEST_FINAL_PATH)

        logger.info("Manifest generation completed successfully")
        logger.info("Total scanned objects=%d", total_scanned)
        logger.info("Total included objects=%d", total_included)
        logger.info("Total excluded objects=%d", total_excluded)
        logger.info("Output file path=%s", MANIFEST_FINAL_PATH.resolve())
        return 0

    except (ClientError, BotoCoreError) as exc:
        logger.exception("AWS error while generating manifest: %s", exc)
        return 1
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unexpected error while generating manifest: %s", exc)
        return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate S3 Batch manifest CSV")
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Run in background mode and return immediately",
    )
    args = parser.parse_args()

    if args.daemon and os.getenv("GENERATE_MANIFEST_DAEMON") != "1":
        stdout_log = STDOUT_LOG_PATH.open("a", encoding="utf-8")
        cmd = [sys.executable, str(Path(__file__).resolve())]
        env = {**os.environ, "GENERATE_MANIFEST_DAEMON": "1"}
        proc = subprocess.Popen(
            cmd,
            stdout=stdout_log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=env,
        )
        stdout_log.close()
        STATE_PATH.write_text(
            (
                "{\n"
                f'  "daemon_pid": {proc.pid},\n'
                f'  "stdout_log": "{STDOUT_LOG_PATH.resolve()}",\n'
                f'  "app_log": "{LOG_PATH.resolve()}"\n'
                "}\n"
            ),
            encoding="utf-8",
        )
        print(f"Manifest daemon started. PID={proc.pid}")
        print(f"Stdout log: {STDOUT_LOG_PATH.resolve()}")
        print(f"App log: {LOG_PATH.resolve()}")
        return 0

    logger = setup_logging()
    logger.info("Script start")
    return generate_manifest(logger)


if __name__ == "__main__":
    sys.exit(main())
