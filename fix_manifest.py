#!/usr/bin/env python3
"""Fix malformed S3 Batch manifest rows by URL-encoding object keys."""

from __future__ import annotations

import argparse
import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import quote


LOG_FILE_NAME = "fix_manifest.log"
PROGRESS_EVERY = 100_000


def setup_logging() -> logging.Logger:
    """Configure timestamped logging to console and local file."""
    logger = logging.getLogger("fix_manifest")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(LOG_FILE_NAME, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def derive_output_path(input_path: Path, output_path: Optional[Path], in_place: bool) -> Path:
    """Resolve output path from CLI options."""
    if in_place:
        # Real output path is handled through temp + atomic replace.
        return input_path
    if output_path is not None:
        return output_path
    return input_path.with_name(f"{input_path.stem}.fixed.csv")


def split_row(line: str) -> Optional[Tuple[str, str]]:
    """
    Split malformed row into bucket and raw key using first comma only.

    Returns:
      (bucket, key) when valid, otherwise None.
    """
    if "," not in line:
        return None
    bucket, key = line.split(",", 1)
    return bucket, key


def process_file(
    input_path: Path,
    output_path: Path,
    in_place: bool,
    logger: logging.Logger,
) -> int:
    """Stream input manifest, write fixed rows, and return exit code."""
    total_lines_read = 0
    total_rows_written = 0
    total_invalid_rows = 0
    total_rows_changed = 0

    temp_output_path: Optional[Path] = None

    try:
        if in_place:
            fd, temp_name = tempfile.mkstemp(
                prefix=f"{input_path.name}.",
                suffix=".tmp",
                dir=str(input_path.parent),
                text=True,
            )
            os.close(fd)
            temp_output_path = Path(temp_name)
            target_output_path = temp_output_path
        else:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            target_output_path = output_path

        logger.info("Starting manifest fix")
        logger.info("Input path: %s", input_path.resolve())
        logger.info("Output path: %s", output_path.resolve())
        logger.info("In-place mode: %s", in_place)

        with input_path.open("r", encoding="utf-8", newline="") as src, target_output_path.open(
            "w", encoding="utf-8", newline=""
        ) as dst:
            for line_number, raw_line in enumerate(src, start=1):
                total_lines_read += 1
                stripped = raw_line.rstrip("\r\n")

                if not stripped:
                    continue

                parsed = split_row(stripped)
                if parsed is None:
                    total_invalid_rows += 1
                    logger.warning("Invalid row at line %d: missing comma", line_number)
                    continue

                bucket, raw_key = parsed
                encoded_key = quote(raw_key, safe="/")
                if encoded_key != raw_key:
                    total_rows_changed += 1

                dst.write(f"{bucket},{encoded_key}\n")
                total_rows_written += 1

                if total_lines_read % PROGRESS_EVERY == 0:
                    logger.info(
                        "Progress lines_read=%d rows_written=%d invalid_rows=%d changed_rows=%d",
                        total_lines_read,
                        total_rows_written,
                        total_invalid_rows,
                        total_rows_changed,
                    )

            dst.flush()
            os.fsync(dst.fileno())

        if in_place:
            assert temp_output_path is not None
            os.replace(temp_output_path, input_path)

        logger.info("Manifest fix completed successfully")
        logger.info("Summary input_path=%s", input_path.resolve())
        logger.info("Summary output_path=%s", output_path.resolve())
        logger.info("Summary total_lines_read=%d", total_lines_read)
        logger.info("Summary total_rows_written=%d", total_rows_written)
        logger.info("Summary total_invalid_rows=%d", total_invalid_rows)
        logger.info("Summary total_rows_changed=%d", total_rows_changed)

        print(f"input_path={input_path.resolve()}")
        print(f"output_path={output_path.resolve()}")
        print(f"total_lines_read={total_lines_read}")
        print(f"total_rows_written={total_rows_written}")
        print(f"total_invalid_rows={total_invalid_rows}")
        print(f"total_rows_changed={total_rows_changed}")
        return 0

    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fix manifest: %s", exc)
        if temp_output_path and temp_output_path.exists():
            try:
                temp_output_path.unlink()
            except OSError:
                logger.warning("Could not remove temp file: %s", temp_output_path)
        return 1


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Fix malformed S3 manifest keys by URL-encoding.")
    parser.add_argument("--path", required=True, type=Path, help="Input manifest CSV path.")
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output fixed CSV path. Ignored when --in-place is used.",
    )
    parser.add_argument(
        "--in-place",
        action="store_true",
        help="Write through temp file and atomically replace original on success.",
    )
    args = parser.parse_args()

    if args.in_place and args.output is not None:
        parser.error("--in-place cannot be used together with --output")
    return args


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    logger = setup_logging()

    input_path = args.path.expanduser()
    if not input_path.exists():
        logger.error("Input file does not exist: %s", input_path)
        return 1
    if not input_path.is_file():
        logger.error("Input path is not a file: %s", input_path)
        return 1

    output_path = derive_output_path(
        input_path=input_path,
        output_path=args.output.expanduser() if args.output else None,
        in_place=args.in_place,
    )

    return process_file(
        input_path=input_path,
        output_path=output_path,
        in_place=args.in_place,
        logger=logger,
    )


if __name__ == "__main__":
    sys.exit(main())
