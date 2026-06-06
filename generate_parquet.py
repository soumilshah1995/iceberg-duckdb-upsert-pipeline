#!/usr/bin/env python3
"""Generate parquet customer files into S3 raw prefix for lab2."""

from __future__ import annotations

import argparse
import io
import random
import time

import pyarrow as pa
import pyarrow.parquet as pq

from aws_session import s3_client
from config import BUCKET, RAW_PREFIX, URI_SCHEME, validate_config

CITIES = ["Boston", "Seattle", "Austin", "Denver", "Miami", "Chicago", "Portland"]
NAMES = ["Alice", "Bob", "Carol", "Dan", "Eve", "Frank", "Grace", "Henry"]


def _make_table(start_id: int, row_count: int) -> pa.Table:
    ids = list(range(start_id, start_id + row_count))
    return pa.table(
        {
            "customer_id": ids,
            "name": [random.choice(NAMES) for _ in ids],
            "city": [random.choice(CITIES) for _ in ids],
            "balance": [round(random.uniform(50, 500), 2) for _ in ids],
        }
    )


def _target_row_count(target_mb: float) -> int:
    # Snappy parquet ~120 bytes/row for this schema; pad for safety
    return max(500, int(target_mb * 1024 * 1024 / 120))


def upload_parquet(s3, key: str, table: pa.Table, target_bytes: int) -> tuple[int, pa.Table]:
    """Write parquet, growing row batches until target size is reached."""
    rows = len(table)
    while True:
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        size = buf.tell()
        if size >= target_bytes or rows >= 500_000:
            s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
            return size, table
        # Double rows and regenerate (contiguous ids from same start_id)
        rows *= 2
        start_id = table["customer_id"][0].as_py()
        table = _make_table(start_id, rows)


def generate(
    file_count: int = 5,
    target_mb_per_file: float = 3.0,
    start_id: int = 1000,
    profile: str | None = None,
) -> list[str]:
    """
    Write parquet files to s3://{bucket}/{raw_prefix}.
    Default: 5 files × ~3 MB ≈ 15 MB total (triggers one 10 MB manifest batch).
    """
    try:
        s3 = s3_client(profile)
    except SystemExit:
        raise
    row_count = _target_row_count(target_mb_per_file)

    uploaded: list[str] = []
    next_id = start_id

    for i in range(file_count):
        table = _make_table(next_id, row_count)
        ts = int(time.time() * 1000)
        key = f"{RAW_PREFIX}customers_{ts}_{i}.parquet"
        target_bytes = int(target_mb_per_file * 1024 * 1024)
        size, table = upload_parquet(s3, key, table, target_bytes)
        actual_rows = len(table)
        uri = f"{URI_SCHEME}{BUCKET}/{key}"
        uploaded.append(uri)
        print(f"Uploaded {uri} ({size / (1024 * 1024):.2f} MB, {actual_rows} rows)")
        next_id += actual_rows

    return uploaded


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate raw parquet files on S3")
    parser.add_argument("--files", type=int, default=5, help="Number of parquet files")
    parser.add_argument(
        "--mb-per-file",
        type=float,
        default=3.0,
        help="Target size per file in MB (approx)",
    )
    parser.add_argument("--start-id", type=int, default=1000, help="Starting customer_id")
    parser.add_argument(
        "--profile",
        default=None,
        help="AWS profile (optional; uses LAB2_AWS_PROFILE / AWS_PROFILE / default chain)",
    )
    args = parser.parse_args()
    validate_config()

    uris = generate(
        file_count=args.files,
        target_mb_per_file=args.mb_per_file,
        start_id=args.start_id,
        profile=args.profile,
    )
    print(f"\nGenerated {len(uris)} files under s3://{BUCKET}/{RAW_PREFIX}")


if __name__ == "__main__":
    main()
