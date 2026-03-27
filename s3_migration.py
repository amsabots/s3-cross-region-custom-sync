#!/usr/bin/env python3
"""
S3 migration orchestrator that uses AWS CLI for data movement.

Design highlights:
- Python handles discovery, queueing, worker/process lifecycle, state, and logs.
- AWS CLI (`aws s3 sync` / `aws s3 cp`) performs all transfers.
- Status model is explicit:
  QUEUED -> STARTING -> RUNNING -> (SUCCESS | PARTIAL | FAILED | INTERRUPTED)
- A process disappearing from `ps`/`grep` can simply mean it exited normally.
- Non-zero `aws s3 sync` can still be PARTIAL if some objects copied but some failed.
- Copied-object counts are derived from CLI output and can be unknown in quiet mode.
"""

from __future__ import annotations

import argparse
from contextlib import nullcontext
import json
import logging
import os
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

try:
    import fcntl
except ImportError as exc:
    raise RuntimeError("This script currently requires Linux/Unix (fcntl unavailable)") from exc


SOURCE_BUCKET = "bh-pl-prod-static"
DEST_BUCKET = "bh-pl-prod-static-mumbai"
SOURCE_REGION = "me-south-1"
DEST_REGION = "ap-south-1"
DEFAULT_MAX_PARALLEL = 5
DEFAULT_WORKDIR = Path.home() / "s3-migration"
FAILED_KEYS_DIRNAME = "failed-keys"
LOCK_FILE = "master_parallel.lock"

FAILED_PATTERNS = (
    "copy failed:",
    "InternalError",
    "SlowDown",
)

AWS_RETRY_ENV = {
    "AWS_RETRY_MODE": "adaptive",
    "AWS_MAX_ATTEMPTS": "1",
}


class Status:
    QUEUED = "QUEUED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    INTERRUPTED = "INTERRUPTED"


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def now_local_human() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def sanitize_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", name.strip())


def safe_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=str(path.parent), delete=False
    ) as tmp:
        json.dump(payload, tmp, indent=2, sort_keys=True)
        tmp.write("\n")
        temp_name = tmp.name
    os.replace(temp_name, path)


def read_text_if_exists(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


@dataclass
class Job:
    job_id: str
    display_name: str
    command: List[str]
    raw_log_path: Path
    summary_log_path: Path
    state_path: Path
    source_prefix: str
    is_upload_prefix: bool
    upload_folder_name: Optional[str] = None
    failed_keys_file: Optional[Path] = None
    status: str = Status.QUEUED
    pid: Optional[int] = None
    start_ts: Optional[str] = None
    end_ts: Optional[str] = None
    duration_seconds: Optional[float] = None
    exit_code: Optional[int] = None
    copied_objects: Optional[int] = None
    failed_objects: int = 0
    total_bytes_transferred: Optional[int] = None
    exception: Optional[str] = None

    def as_state_dict(self) -> Dict[str, Any]:
        return {
            "source_bucket": SOURCE_BUCKET,
            "destination_bucket": DEST_BUCKET,
            "source_region": SOURCE_REGION,
            "destination_region": DEST_REGION,
            "prefix": self.source_prefix,
            "pid": self.pid,
            "status": self.status,
            "start_timestamp": self.start_ts,
            "end_timestamp": self.end_ts,
            "duration_seconds": self.duration_seconds,
            "exit_code": self.exit_code,
            "raw_log_path": str(self.raw_log_path),
            "summary_log_path": str(self.summary_log_path),
            "failed_keys_file_path": str(self.failed_keys_file) if self.failed_keys_file else None,
            "failed_object_count": self.failed_objects,
            "copied_object_count": self.copied_objects if self.copied_objects is not None else "unknown",
            "total_bytes_transferred": self.total_bytes_transferred,
            "job_id": self.job_id,
            "display_name": self.display_name,
            "is_upload_prefix": self.is_upload_prefix,
            "upload_folder_name": self.upload_folder_name,
            "command": self.command,
            "exception": self.exception,
        }


class LockedRunGuard:
    """Simple non-blocking lock to avoid overlapping master runs."""

    def __init__(self, lock_path: Path) -> None:
        self.lock_path = lock_path
        self._fh: Optional[Any] = None

    def __enter__(self) -> "LockedRunGuard":
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise RuntimeError(
                f"Another migration run appears active (lock: {self.lock_path})"
            ) from None
        self._fh.seek(0)
        self._fh.truncate()
        self._fh.write(f"pid={os.getpid()}\nstarted_at={now_utc_iso()}\n")
        self._fh.flush()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        if self._fh:
            try:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            finally:
                self._fh.close()


class MigrationOrchestrator:
    def __init__(
        self,
        workdir: Path,
        max_parallel: int,
        quiet_mode: bool,
        skip_successful: bool,
        skip_folders: Optional[Set[str]] = None,
        allow_concurrent_run: bool = False,
    ) -> None:
        self.workdir = workdir
        self.failed_keys_dir = self.workdir / FAILED_KEYS_DIRNAME
        self.max_parallel = max_parallel
        self.quiet_mode = quiet_mode
        self.skip_successful = skip_successful
        self.skip_folders = skip_folders or set()
        self.allow_concurrent_run = allow_concurrent_run
        self.master_raw_log = self.workdir / "master_parallel.raw.log"
        self.master_summary_log = self.workdir / "master_parallel.summary.log"
        self.master_error_log = self.workdir / "master_parallel.errors.log"
        self.master_state_file = self.workdir / "master_parallel.state.json"
        self.lock_path = self.workdir / LOCK_FILE
        self._state_lock = threading.Lock()
        self._log_lock = threading.Lock()
        self._jobs_by_id: Dict[str, Job] = {}
        self._completed_jobs_ordered: List[str] = []
        self._master_state: Dict[str, Any] = {
            "source_bucket": SOURCE_BUCKET,
            "destination_bucket": DEST_BUCKET,
            "source_region": SOURCE_REGION,
            "destination_region": DEST_REGION,
            "workdir": str(self.workdir),
            "pid": os.getpid(),
            "started_at": now_utc_iso(),
            "queued_jobs": [],
            "running_jobs": [],
            "completed_jobs": [],
            "failed_jobs": [],
            "interrupted_jobs": [],
            "summary_counters": {},
        }
        self._master_logger = logging.getLogger("master_summary")
        self._error_logger = logging.getLogger("master_errors")
        self._configure_master_loggers()

    def _configure_master_loggers(self) -> None:
        self.workdir.mkdir(parents=True, exist_ok=True)
        self.failed_keys_dir.mkdir(parents=True, exist_ok=True)

        self._master_logger.handlers.clear()
        self._master_logger.setLevel(logging.INFO)
        summary_handler = logging.FileHandler(self.master_summary_log, encoding="utf-8")
        summary_handler.setFormatter(logging.Formatter("%(message)s"))
        self._master_logger.addHandler(summary_handler)
        self._master_logger.propagate = False

        self._error_logger.handlers.clear()
        self._error_logger.setLevel(logging.INFO)
        error_handler = logging.FileHandler(self.master_error_log, encoding="utf-8")
        error_handler.setFormatter(logging.Formatter("%(message)s"))
        self._error_logger.addHandler(error_handler)
        self._error_logger.propagate = False

    def log_master(self, message: str) -> None:
        line = f"[{now_local_human()}] {message}"
        with self._log_lock:
            self._master_logger.info(line)
        print(line)

    def log_error(self, folder_or_job: str, message: str) -> None:
        line = f"[{now_local_human()}] [{folder_or_job}] {message}"
        with self._log_lock:
            self._error_logger.info(line)

    def log_job_summary(self, job: Job, message: str) -> None:
        line = f"[{now_local_human()}] [{job.display_name}] {message}"
        with self._log_lock:
            with job.summary_log_path.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")
            self._master_logger.info(line)
        print(line)

    def write_master_state(self) -> None:
        with self._state_lock:
            safe_write_json(self.master_state_file, self._master_state)

    def set_daemon_pid(self, pid: int) -> None:
        self._master_state["daemon_pid"] = pid
        self.write_master_state()

    def _status_counts(self) -> Dict[str, int]:
        counts = {
            Status.QUEUED: 0,
            Status.STARTING: 0,
            Status.RUNNING: 0,
            Status.SUCCESS: 0,
            Status.PARTIAL: 0,
            Status.FAILED: 0,
            Status.INTERRUPTED: 0,
        }
        for job in self._jobs_by_id.values():
            counts[job.status] = counts.get(job.status, 0) + 1
        return counts

    def _refresh_master_lists(self) -> None:
        queued: List[str] = []
        running: List[str] = []
        completed: List[str] = []
        failed: List[str] = []
        interrupted: List[str] = []
        for job in self._jobs_by_id.values():
            if job.status in (Status.QUEUED, Status.STARTING):
                queued.append(job.job_id)
            elif job.status == Status.RUNNING:
                running.append(job.job_id)
            elif job.status in (Status.SUCCESS, Status.PARTIAL, Status.FAILED, Status.INTERRUPTED):
                completed.append(job.job_id)
            if job.status == Status.FAILED:
                failed.append(job.job_id)
            if job.status == Status.INTERRUPTED:
                interrupted.append(job.job_id)
        self._master_state["queued_jobs"] = queued
        self._master_state["running_jobs"] = running
        self._master_state["completed_jobs"] = completed
        self._master_state["failed_jobs"] = failed
        self._master_state["interrupted_jobs"] = interrupted
        self._master_state["summary_counters"] = self._status_counts()
        self._master_state["completed_in_order"] = self._completed_jobs_ordered
        self.write_master_state()

    def _append_master_counter_line(self) -> None:
        counts = self._status_counts()
        total = len(self._jobs_by_id)
        remaining = counts[Status.QUEUED] + counts[Status.STARTING] + counts[Status.RUNNING]
        self.log_master(
            "QUEUED={q} RUNNING={r} SUCCESS={s} PARTIAL={p} FAILED={f} INTERRUPTED={i} "
            "REMAINING={rem}/{total}".format(
                q=counts[Status.QUEUED] + counts[Status.STARTING],
                r=counts[Status.RUNNING],
                s=counts[Status.SUCCESS],
                p=counts[Status.PARTIAL],
                f=counts[Status.FAILED],
                i=counts[Status.INTERRUPTED],
                rem=remaining,
                total=total,
            )
        )

    def discover_upload_folders(self) -> List[str]:
        cmd = [
            "aws",
            "s3",
            "ls",
            f"s3://{SOURCE_BUCKET}/upload/",
            "--region",
            SOURCE_REGION,
        ]
        self.log_master(f"Discovering upload prefixes via: {' '.join(cmd)}")
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                env={**os.environ, **AWS_RETRY_ENV},
            )
        except subprocess.CalledProcessError as exc:
            self.log_error("discovery", f"AWS CLI discovery failed rc={exc.returncode}")
            self.log_error("discovery", (exc.stderr or "").strip() or "stderr empty")
            raise

        prefixes: List[str] = []
        for line in result.stdout.splitlines():
            # Typical format: "                           PRE dtcm-invoice/"
            if "PRE " not in line:
                continue
            maybe = line.split("PRE ", 1)[1].strip()
            if maybe.endswith("/"):
                maybe = maybe[:-1]
            if not maybe or maybe == "_tmp":
                continue
            if maybe in self.skip_folders:
                continue
            prefixes.append(maybe)
        prefixes = sorted(set(prefixes))
        skipped_display = ", ".join(sorted(self.skip_folders)) if self.skip_folders else "none"
        self.log_master(
            f"Discovered {len(prefixes)} upload folders (excluding _tmp, skipped: {skipped_display})"
        )
        return prefixes

    def build_jobs(self, upload_folders: Sequence[str]) -> Tuple[List[Job], List[Job]]:
        serial_jobs: List[Job] = []
        parallel_jobs: List[Job] = []

        serial_jobs.append(self._build_sync_job("event-info", "fonts", "fonts", is_upload_prefix=False))
        serial_jobs.append(self._build_cp_job("event-info", "404.html", "404.html", is_upload_prefix=False))

        for folder in upload_folders:
            parallel_jobs.append(
                self._build_sync_job(
                    display_name=folder,
                    source_prefix=f"upload/{folder}",
                    destination_prefix=f"upload/{folder}",
                    is_upload_prefix=True,
                    upload_folder_name=folder,
                )
            )
        return serial_jobs, parallel_jobs

    def _build_sync_job(
        self,
        display_name: str,
        source_prefix: str,
        destination_prefix: str,
        is_upload_prefix: bool,
        upload_folder_name: Optional[str] = None,
    ) -> Job:
        safe = sanitize_name(upload_folder_name or display_name)
        suffix = f"{safe}_parallel" if is_upload_prefix else f"{safe}_special_parallel"
        raw_log = self.workdir / f"{suffix}.raw.log"
        summary_log = self.workdir / f"{suffix}.summary.log"
        state_path = self.workdir / f"{suffix}.state.json"
        failed_keys_file = (
            self.failed_keys_dir / f"upload_{sanitize_name(upload_folder_name or display_name)}.txt"
            if is_upload_prefix
            else None
        )
        cmd = [
            "aws",
            "s3",
            "sync",
            f"s3://{SOURCE_BUCKET}/{source_prefix}",
            f"s3://{DEST_BUCKET}/{destination_prefix}",
            "--source-region",
            SOURCE_REGION,
            "--region",
            DEST_REGION,
        ]
        if self.quiet_mode:
            cmd.append("--only-show-errors")
        return Job(
            job_id=f"sync:{source_prefix}",
            display_name=display_name,
            command=cmd,
            raw_log_path=raw_log,
            summary_log_path=summary_log,
            state_path=state_path,
            source_prefix=source_prefix,
            is_upload_prefix=is_upload_prefix,
            upload_folder_name=upload_folder_name,
            failed_keys_file=failed_keys_file,
        )

    def _build_cp_job(
        self,
        display_name: str,
        source_key: str,
        destination_key: str,
        is_upload_prefix: bool,
    ) -> Job:
        safe = sanitize_name(display_name)
        suffix = f"{safe}_special_parallel"
        raw_log = self.workdir / f"{suffix}.raw.log"
        summary_log = self.workdir / f"{suffix}.summary.log"
        state_path = self.workdir / f"{suffix}.state.json"
        cmd = [
            "aws",
            "s3",
            "cp",
            f"s3://{SOURCE_BUCKET}/{source_key}",
            f"s3://{DEST_BUCKET}/{destination_key}",
            "--source-region",
            SOURCE_REGION,
            "--region",
            DEST_REGION,
        ]
        if self.quiet_mode:
            cmd.append("--only-show-errors")
        return Job(
            job_id=f"cp:{source_key}",
            display_name=display_name,
            command=cmd,
            raw_log_path=raw_log,
            summary_log_path=summary_log,
            state_path=state_path,
            source_prefix=source_key,
            is_upload_prefix=is_upload_prefix,
        )

    def maybe_skip_job(self, job: Job) -> bool:
        if not self.skip_successful or not job.state_path.exists():
            return False
        try:
            existing = json.loads(job.state_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return False
        if existing.get("status") == Status.SUCCESS:
            job.status = Status.SUCCESS
            job.start_ts = now_utc_iso()
            job.end_ts = now_utc_iso()
            job.duration_seconds = 0.0
            job.exit_code = 0
            self._jobs_by_id[job.job_id] = job
            self._completed_jobs_ordered.append(job.job_id)
            safe_write_json(job.state_path, job.as_state_dict())
            self.log_job_summary(job, "SKIPPED existing SUCCESS state (safe resume)")
            self._refresh_master_lists()
            return True
        return False

    def execute_job(self, job: Job) -> Job:
        self._jobs_by_id[job.job_id] = job
        self._refresh_master_lists()
        self.log_job_summary(job, "QUEUED")

        if self.maybe_skip_job(job):
            return job

        env = {**os.environ, **AWS_RETRY_ENV}
        start_monotonic = time.monotonic()

        job.status = Status.STARTING
        job.start_ts = now_utc_iso()
        safe_write_json(job.state_path, job.as_state_dict())
        self._refresh_master_lists()
        self.log_job_summary(job, "STARTING pid=pending")

        try:
            with job.raw_log_path.open("a", encoding="utf-8") as raw_fh, self.master_raw_log.open(
                "a", encoding="utf-8"
            ) as master_raw:
                raw_fh.write(
                    f"[{now_local_human()}] [command] {' '.join(job.command)}\n"
                )
                master_raw.write(
                    f"[{now_local_human()}] [{job.display_name}] [command] {' '.join(job.command)}\n"
                )
                raw_fh.flush()
                master_raw.flush()

                proc = subprocess.Popen(
                    job.command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    env=env,
                    bufsize=1,
                )
                job.pid = proc.pid
                job.status = Status.RUNNING
                safe_write_json(job.state_path, job.as_state_dict())
                self._refresh_master_lists()
                self.log_job_summary(job, f"RUNNING pid={job.pid}")

                assert proc.stdout is not None
                for line in proc.stdout:
                    raw_fh.write(line)
                    master_raw.write(f"[{job.display_name}] {line}")
                    if any(pattern in line for pattern in FAILED_PATTERNS):
                        self.log_error(job.display_name, line.strip())
                proc.wait()
                job.exit_code = proc.returncode
        except Exception as exc:  # noqa: BLE001
            job.exception = f"{type(exc).__name__}: {exc}"
            job.exit_code = 1
            self.log_error(job.display_name, f"unexpected exception: {job.exception}")

        job.end_ts = now_utc_iso()
        job.duration_seconds = round(time.monotonic() - start_monotonic, 3)
        parsed = self.parse_raw_log(job)
        job.failed_objects = parsed["failed_count"]
        job.copied_objects = parsed["copied_count"]
        job.total_bytes_transferred = parsed["total_bytes"]

        if job.is_upload_prefix:
            failed_keys = sorted(parsed["failed_keys"])
            if failed_keys and job.failed_keys_file:
                job.failed_keys_file.parent.mkdir(parents=True, exist_ok=True)
                job.failed_keys_file.write_text("\n".join(failed_keys) + "\n", encoding="utf-8")
                self.log_error(
                    job.display_name,
                    f"failed_objects={len(failed_keys)} failed_keys_file={job.failed_keys_file}",
                )
            elif job.failed_keys_file and job.failed_keys_file.exists():
                # Keep reruns clean and avoid stale retry input.
                job.failed_keys_file.unlink()

        job.status = self.classify_status(job)
        safe_write_json(job.state_path, job.as_state_dict())
        self._completed_jobs_ordered.append(job.job_id)
        self._refresh_master_lists()

        copied_display = str(job.copied_objects) if job.copied_objects is not None else "unknown"
        failed_keys_part = (
            f" failed_keys_file={job.failed_keys_file}" if (job.failed_objects > 0 and job.failed_keys_file) else ""
        )
        self.log_job_summary(
            job,
            (
                f"{job.status} pid={job.pid} duration={int(job.duration_seconds or 0)}s "
                f"exit_code={job.exit_code} copied_objects={copied_display} "
                f"failed_objects={job.failed_objects}{failed_keys_part}"
            ),
        )
        if job.status in (Status.FAILED, Status.INTERRUPTED):
            self.log_error(
                job.display_name,
                f"terminal_status={job.status} pid={job.pid} exit_code={job.exit_code}",
            )
        return job

    def classify_status(self, job: Job) -> str:
        # A process may disappear from process listings because it exited normally.
        if job.exit_code is None:
            return Status.FAILED
        if job.exit_code < 0:
            return Status.INTERRUPTED
        if job.exit_code in (128 + signal.SIGTERM, 128 + signal.SIGINT):
            return Status.INTERRUPTED
        if job.exit_code == 0 and job.failed_objects == 0:
            return Status.SUCCESS
        if job.failed_objects > 0:
            # Non-zero sync may still be PARTIAL when some files copied but some failed.
            return Status.PARTIAL
        if job.exit_code == 0 and job.failed_objects > 0:
            return Status.PARTIAL
        return Status.FAILED

    def parse_raw_log(self, job: Job) -> Dict[str, Any]:
        text = read_text_if_exists(job.raw_log_path)
        lines = text.splitlines()
        failed_keys: Set[str] = set()
        failed_count = 0
        copied_count = 0
        total_bytes: Optional[int] = None

        # In non-quiet mode AWS CLI usually prints "copy: s3://... to ..."
        # In quiet mode (`--only-show-errors`), copy lines are absent, so count is unknown.
        copy_line_re = re.compile(r"^\s*copy:\s+s3://")
        copy_failed_re = re.compile(r"copy failed:\s+s3://[^/]+/(.+?)\s+to\s+s3://")
        bytes_re = re.compile(r"Total transferred:\s+([0-9]+)\s+bytes")

        for line in lines:
            if copy_line_re.search(line):
                copied_count += 1
            if any(pattern in line for pattern in FAILED_PATTERNS):
                failed_count += 1
            match = copy_failed_re.search(line)
            if match:
                key = match.group(1).strip()
                if job.is_upload_prefix and job.upload_folder_name:
                    prefix = f"upload/{job.upload_folder_name}/"
                    if key.startswith(prefix):
                        failed_keys.add(key)
                elif not job.is_upload_prefix:
                    failed_keys.add(key)
            bytes_match = bytes_re.search(line)
            if bytes_match:
                try:
                    total_bytes = int(bytes_match.group(1))
                except ValueError:
                    pass

        return {
            "failed_count": len(failed_keys) if failed_keys else failed_count,
            "failed_keys": failed_keys,
            "copied_count": copied_count if copied_count > 0 else None,
            "total_bytes": total_bytes,
        }

    def run(self) -> int:
        run_guard = nullcontext() if self.allow_concurrent_run else LockedRunGuard(self.lock_path)
        with run_guard:
            self._master_state["pid"] = os.getpid()
            self._master_state["started_at"] = now_utc_iso()
            self.write_master_state()

            self.log_master("Starting S3 migration orchestrator")
            skip_folders_display = ", ".join(sorted(self.skip_folders)) if self.skip_folders else "none"
            if self.allow_concurrent_run:
                self.log_master(
                    "allow_concurrent_run=True: master lock disabled for this run; "
                    "use only with --skip-folders to avoid overlapping same prefixes"
                )
            self.log_master(
                f"Config max_parallel={self.max_parallel} quiet_mode={self.quiet_mode} "
                f"skip_successful={self.skip_successful} skip_folders=[{skip_folders_display}] "
                f"allow_concurrent_run={self.allow_concurrent_run}"
            )

            upload_folders = self.discover_upload_folders()
            serial_jobs, parallel_jobs = self.build_jobs(upload_folders)

            for job in serial_jobs + parallel_jobs:
                self._jobs_by_id[job.job_id] = job
            self._refresh_master_lists()
            self._append_master_counter_line()

            # Handle fonts/404 first as requested.
            for job in serial_jobs:
                self.execute_job(job)
                self._append_master_counter_line()

            futures = []
            if parallel_jobs:
                with ThreadPoolExecutor(max_workers=self.max_parallel) as executor:
                    for job in parallel_jobs:
                        futures.append(executor.submit(self.execute_job, job))
                    # Completion reporting in real completion order.
                    for future in as_completed(futures):
                        finished_job = future.result()
                        self.log_master(
                            f"completed_in_order job={finished_job.display_name} status={finished_job.status}"
                        )
                        self._append_master_counter_line()

            self._master_state["finished_at"] = now_utc_iso()
            self._refresh_master_lists()
            self.log_master("Migration run completed")

            counts = self._status_counts()
            if counts[Status.FAILED] > 0 or counts[Status.INTERRUPTED] > 0:
                return 1
            return 0


class RetryRunner:
    def __init__(self, workdir: Path, quiet_mode: bool) -> None:
        self.workdir = workdir
        self.quiet_mode = quiet_mode
        self.failed_keys_dir = self.workdir / FAILED_KEYS_DIRNAME
        self.master_error_log = self.workdir / "master_parallel.errors.log"
        self.retry_master_raw_log = self.workdir / "retry_parallel.raw.log"
        self.retry_master_summary_log = self.workdir / "retry_parallel.summary.log"

    def _log(self, path: Path, line: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(f"[{now_local_human()}] {line}\n")

    def _log_retry_master(self, line: str) -> None:
        self._log(self.retry_master_summary_log, line)

    def _extract_folder_from_failed_file(self, failed_file: Path) -> str:
        stem = failed_file.stem
        if stem.startswith("upload_"):
            return stem[len("upload_") :]
        return sanitize_name(stem)

    def _resolve_failed_file(self, folder: Optional[str], failed_file: Optional[Path]) -> Path:
        if failed_file:
            return failed_file.expanduser()
        if not folder:
            raise ValueError("Provide --folder or --failed-file")
        safe = sanitize_name(folder)
        return self.failed_keys_dir / f"upload_{safe}.txt"

    def _retry_from_failed_path(
        self,
        failed_path: Path,
        folder_override: Optional[str] = None,
        delete_failed_file_on_success: bool = False,
    ) -> int:
        if not failed_path.exists():
            raise FileNotFoundError(f"Failed-keys file not found: {failed_path}")

        resolved_folder = folder_override or self._extract_folder_from_failed_file(failed_path)
        safe_folder = sanitize_name(resolved_folder)
        raw_log = self.workdir / f"retry_{safe_folder}.raw.log"
        summary_log = self.workdir / f"retry_{safe_folder}.summary.log"

        keys = [
            line.strip()
            for line in failed_path.read_text(encoding="utf-8", errors="replace").splitlines()
            if line.strip()
        ]
        keys = sorted(set(keys))
        self._log(summary_log, f"Starting retry for folder={resolved_folder} keys={len(keys)}")
        self._log_retry_master(
            f"[{resolved_folder}] STARTING keys={len(keys)} failed_file={failed_path}"
        )

        if not keys:
            self._log(summary_log, "No keys to retry (empty failed-file)")
            self._log_retry_master(f"[{resolved_folder}] No keys to retry (empty failed-file)")
            return 0

        env = {**os.environ, **AWS_RETRY_ENV}
        successes = 0
        failures = 0

        with raw_log.open("a", encoding="utf-8") as raw_fh, self.retry_master_raw_log.open(
            "a", encoding="utf-8"
        ) as master_raw_fh:
            for idx, key in enumerate(keys, start=1):
                cmd = [
                    "aws",
                    "s3",
                    "cp",
                    f"s3://{SOURCE_BUCKET}/{key}",
                    f"s3://{DEST_BUCKET}/{key}",
                    "--source-region",
                    SOURCE_REGION,
                    "--region",
                    DEST_REGION,
                ]
                if self.quiet_mode:
                    cmd.append("--only-show-errors")
                cmd_line = f"[{now_local_human()}] [retry-command] {' '.join(cmd)}\n"
                raw_fh.write(cmd_line)
                master_raw_fh.write(f"[{resolved_folder}] {cmd_line}")
                raw_fh.flush()
                master_raw_fh.flush()
                proc = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    env=env,
                )
                if proc.stdout:
                    raw_fh.write(proc.stdout)
                    master_raw_fh.write(f"[{resolved_folder}] {proc.stdout}")
                if proc.stderr:
                    raw_fh.write(proc.stderr)
                    master_raw_fh.write(f"[{resolved_folder}] {proc.stderr}")
                if proc.returncode == 0:
                    successes += 1
                else:
                    failures += 1
                    with self.master_error_log.open("a", encoding="utf-8") as err_fh:
                        err_fh.write(
                            f"[{now_local_human()}] [retry-{resolved_folder}] cp failed "
                            f"key={key} exit_code={proc.returncode}\n"
                        )
                if idx % 100 == 0 or idx == len(keys):
                    self._log_retry_master(
                        f"[{resolved_folder}] PROGRESS processed={idx}/{len(keys)} "
                        f"success={successes} failed={failures}"
                    )
                raw_fh.flush()
                master_raw_fh.flush()

        self._log(
            summary_log,
            f"Retry finished folder={resolved_folder} success={successes} failed={failures}",
        )
        self._log_retry_master(
            f"[{resolved_folder}] FINISHED success={successes} failed={failures}"
        )
        rc = 0 if failures == 0 else 1

        # Keep failed-key files by default for audit/replay. Delete only when requested
        # and only after a fully successful retry run for that specific file.
        if rc == 0 and delete_failed_file_on_success:
            try:
                failed_path.unlink(missing_ok=True)
                self._log(summary_log, f"Deleted failed-file after success: {failed_path}")
                self._log_retry_master(
                    f"[{resolved_folder}] Deleted failed-file after success: {failed_path}"
                )
            except OSError as exc:
                self._log(summary_log, f"Could not delete failed-file {failed_path}: {exc}")
                self._log_retry_master(
                    f"[{resolved_folder}] Could not delete failed-file {failed_path}: {exc}"
                )
        return rc

    def run(
        self,
        folder: Optional[str],
        failed_file: Optional[Path],
        all_failed: bool,
        delete_failed_file_on_success: bool,
    ) -> int:
        if all_failed:
            files = sorted(self.failed_keys_dir.glob("upload_*.txt"))
            if not files:
                print(f"No failed-key files found in {self.failed_keys_dir}")
                return 0
            overall_rc = 0
            self._log_retry_master(
                f"Starting --all-failed retry scan files={len(files)} quiet_mode={self.quiet_mode}"
            )
            for path in files:
                if not path.exists() or path.stat().st_size == 0:
                    continue
                rc = self._retry_from_failed_path(
                    failed_path=path,
                    folder_override=None,
                    delete_failed_file_on_success=delete_failed_file_on_success,
                )
                if rc != 0:
                    overall_rc = 1
            self._log_retry_master(f"Finished --all-failed retry overall_rc={overall_rc}")
            return overall_rc

        failed_path = self._resolve_failed_file(folder, failed_file)
        return self._retry_from_failed_path(
            failed_path=failed_path,
            folder_override=folder,
            delete_failed_file_on_success=delete_failed_file_on_success,
        )


def launch_daemon(args: argparse.Namespace, workdir: Path) -> int:
    workdir.mkdir(parents=True, exist_ok=True)
    master_raw = workdir / "master_parallel.raw.log"
    cmd = [sys.executable, str(Path(__file__).resolve()), "run"]
    cmd.extend(["--max-parallel", str(args.max_parallel)])
    cmd.extend(["--workdir", str(workdir)])
    if args.verbose_copy_stats:
        cmd.append("--verbose-copy-stats")
    if args.skip_successful:
        cmd.append("--skip-successful")
    if args.skip_folders:
        cmd.extend(["--skip-folders"] + args.skip_folders)
    if args.allow_concurrent_run:
        cmd.append("--allow-concurrent-run")

    env = {**os.environ, "S3_MIGRATION_DAEMON": "1"}
    with master_raw.open("a", encoding="utf-8") as raw_fh:
        proc = subprocess.Popen(
            cmd,
            stdout=raw_fh,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=env,
        )

    # Keep daemon PID discoverable in master state immediately.
    bootstrap_state = {
        "source_bucket": SOURCE_BUCKET,
        "destination_bucket": DEST_BUCKET,
        "source_region": SOURCE_REGION,
        "destination_region": DEST_REGION,
        "workdir": str(workdir),
        "pid": os.getpid(),
        "daemon_pid": proc.pid,
        "started_at": now_utc_iso(),
        "mode": "daemon-bootstrap",
    }
    safe_write_json(workdir / "master_parallel.state.json", bootstrap_state)

    print(f"Daemon started. PID={proc.pid}")
    print(f"Master summary log: {workdir / 'master_parallel.summary.log'}")
    print(f"Master raw log: {master_raw}")
    return 0


def launch_retry_daemon(args: argparse.Namespace, workdir: Path) -> int:
    workdir.mkdir(parents=True, exist_ok=True)
    retry_master_raw = workdir / "retry_parallel.raw.log"
    retry_master_summary = workdir / "retry_parallel.summary.log"

    cmd = [sys.executable, str(Path(__file__).resolve()), "retry"]
    cmd.extend(["--workdir", str(workdir)])
    if args.folder:
        cmd.extend(["--folder", args.folder])
    if args.failed_file:
        cmd.extend(["--failed-file", str(args.failed_file.expanduser())])
    if args.all_failed:
        cmd.append("--all-failed")
    if args.delete_failed_file_on_success:
        cmd.append("--delete-failed-file-on-success")
    if args.verbose_copy_stats:
        cmd.append("--verbose-copy-stats")

    env = {**os.environ, "S3_MIGRATION_RETRY_DAEMON": "1"}
    with retry_master_raw.open("a", encoding="utf-8") as raw_fh:
        proc = subprocess.Popen(
            cmd,
            stdout=raw_fh,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=env,
        )

    retry_state = {
        "workdir": str(workdir),
        "pid": os.getpid(),
        "daemon_pid": proc.pid,
        "started_at": now_utc_iso(),
        "mode": "retry-daemon-bootstrap",
        "command": cmd,
    }
    safe_write_json(workdir / "retry_parallel.state.json", retry_state)
    with retry_master_summary.open("a", encoding="utf-8") as fh:
        fh.write(
            f"[{now_local_human()}] Daemon started pid={proc.pid} command={' '.join(cmd)}\n"
        )

    print(f"Retry daemon started. PID={proc.pid}")
    print(f"Retry summary log: {retry_master_summary}")
    print(f"Retry raw log: {retry_master_raw}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="S3 migration orchestrator")
    sub = parser.add_subparsers(dest="command", required=True)

    run_cmd = sub.add_parser("run", help="Run migration jobs")
    run_cmd.add_argument("--max-parallel", type=int, default=DEFAULT_MAX_PARALLEL)
    run_cmd.add_argument("--workdir", type=Path, default=DEFAULT_WORKDIR)
    run_cmd.add_argument(
        "--verbose-copy-stats",
        action="store_true",
        help="Disable --only-show-errors so copy lines may be counted",
    )
    run_cmd.add_argument(
        "--skip-successful",
        action="store_true",
        help="Skip jobs whose existing state file records SUCCESS",
    )
    run_cmd.add_argument("--daemon", action="store_true", help="Run in background mode")
    run_cmd.add_argument(
        "--skip-folders",
        nargs="+",
        default=[],
        metavar="FOLDER",
        help="Upload folder names to skip (e.g. --skip-folders users event video)",
    )
    run_cmd.add_argument(
        "--allow-concurrent-run",
        action="store_true",
        help="Disable master run lock to allow another run in parallel (use with --skip-folders)",
    )

    retry_cmd = sub.add_parser("retry", help="Retry failed object keys")
    retry_cmd.add_argument("--folder", type=str, default=None)
    retry_cmd.add_argument("--failed-file", type=Path, default=None)
    retry_cmd.add_argument(
        "--all-failed",
        action="store_true",
        help="Retry all files matching ~/s3-migration/failed-keys/upload_*.txt",
    )
    retry_cmd.add_argument(
        "--delete-failed-file-on-success",
        action="store_true",
        help="Delete each failed-key file only when its retry run fully succeeds",
    )
    retry_cmd.add_argument("--workdir", type=Path, default=DEFAULT_WORKDIR)
    retry_cmd.add_argument("--daemon", action="store_true", help="Run retry in background mode")
    retry_cmd.add_argument(
        "--verbose-copy-stats",
        action="store_true",
        help="Disable --only-show-errors for retry copy output",
    )
    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.command == "run":
        if args.max_parallel <= 0:
            raise ValueError("--max-parallel must be > 0")
        if args.allow_concurrent_run and not args.skip_folders:
            raise ValueError("--allow-concurrent-run requires --skip-folders to reduce overlap risk")
    if args.command == "retry":
        if args.all_failed and (args.folder or args.failed_file):
            raise ValueError("retry --all-failed cannot be combined with --folder/--failed-file")
        if not args.all_failed and not args.folder and not args.failed_file:
            raise ValueError("retry requires --folder, --failed-file, or --all-failed")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    validate_args(args)

    if args.command == "run":
        if args.daemon and os.getenv("S3_MIGRATION_DAEMON") != "1":
            return launch_daemon(args, args.workdir.expanduser())
        orchestrator = MigrationOrchestrator(
            workdir=args.workdir.expanduser(),
            max_parallel=args.max_parallel,
            quiet_mode=not args.verbose_copy_stats,
            skip_successful=args.skip_successful,
            skip_folders=set(args.skip_folders) if args.skip_folders else None,
            allow_concurrent_run=args.allow_concurrent_run,
        )
        return orchestrator.run()

    if args.command == "retry":
        if args.daemon and os.getenv("S3_MIGRATION_RETRY_DAEMON") != "1":
            return launch_retry_daemon(args, args.workdir.expanduser())
        retry = RetryRunner(
            workdir=args.workdir.expanduser(),
            quiet_mode=not args.verbose_copy_stats,
        )
        return retry.run(
            folder=args.folder,
            failed_file=args.failed_file,
            all_failed=args.all_failed,
            delete_failed_file_on_success=args.delete_failed_file_on_success,
        )

    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
