#!/usr/bin/env python3
"""
Lab 2 pipeline: volume-based manifest → DuckDB MERGE INTO Iceberg → archive.

  1. generate_parquet.py  — land raw parquet on S3
  2. FileProcessor        — buffer files until 10 MB, write .pending manifest
  3. DuckDBProcessor      — read manifest files, MERGE INTO Iceberg
  4. FileProcessor        — tag raw files archived, delete manifest
"""

from __future__ import annotations

import argparse
import sys

from duckdb_processor import DuckDBProcessor
from file_processor import FileProcessor
from config import validate_config


def process_batch(processor: FileProcessor, duckdb_proc: DuckDBProcessor) -> bool:
    manifest_uri = processor.create_pending_manifest()
    if not manifest_uri:
        print("No unprocessed files buffered — nothing to do.")
        return False

    file_uris = processor.read_manifest(manifest_uri)
    print(f"Processing manifest with {len(file_uris)} file(s)")

    try:
        duckdb_proc.merge_from_manifest(file_uris)
        processor.archive_files(file_uris)
        processor.delete_manifest(manifest_uri)
        print("Batch succeeded.")
        return True
    except Exception as exc:
        print(f"Batch failed: {exc}", file=sys.stderr)
        processor.error_manifest(manifest_uri)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Run lab2 manifest → MERGE pipeline")
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Keep creating manifests until raw prefix is drained",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="AWS profile (optional; uses LAB2_AWS_PROFILE / AWS_PROFILE / default chain)",
    )
    args = parser.parse_args()
    validate_config()

    processor = FileProcessor(profile=args.profile)
    duckdb_proc = DuckDBProcessor(profile=args.profile)
    duckdb_proc.setup()

    try:
        if args.loop:
            while process_batch(processor, duckdb_proc):
                pass
        else:
            process_batch(processor, duckdb_proc)

        print("\nCurrent Iceberg table:")
        print(duckdb_proc.query_table())
    finally:
        duckdb_proc.close()


if __name__ == "__main__":
    main()
