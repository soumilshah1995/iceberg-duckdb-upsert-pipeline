# Lab 2 — File Processing → DuckDB MERGE → Iceberg

Volume-based manifest pipeline: raw parquet on S3 → 10 MB batch → MERGE INTO Iceberg → archive via S3 tags.
```
CDC/Stream → generate_parquet.py → Raw Parquet (S3)
                    ↓
            run_pipeline.py (orchestrator)
                    ↓
    ┌───────────────────────────────────────────────┐
    │ FileProcessor          │ DuckDBProcessor       │
    │ · scan (skip archived) │ · setup / attach      │
    │ · buffer until 10 MB   │ · read_parquet        │
    │ · create manifest      │ · dedupe ROW_NUMBER   │
    │ · read manifest   ────→│ · MERGE INTO Iceberg  │
    │ · archive + delete     │                       │
    │ · error_manifest (fail)│ → Iceberg table       │
    └───────────────────────────────────────────────┘
                    ↓
            Async job (optional lifecycle archive)
```
---

## Prerequisites

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in your bucket + S3 Tables ARN
aws sso login --profile YOUR_PROFILE
duckdb -version        # v1.5.3+
```

---

## Configuration

Copy `.env.example` → `.env` and set:

| Variable | Required | Description |
|----------|----------|-------------|
| `LAB2_BUCKET` | Yes | S3 bucket for raw parquet + manifests |
| `LAB2_S3_TABLES_ARN` | Yes | Amazon S3 Tables catalog ARN |
| `LAB2_AWS_PROFILE` | No | AWS CLI profile name |
| `LAB2_BUFFER_MB` | No | Manifest volume threshold (default `10`) |

Load env before running:

```bash
export $(grep -v '^#' .env | xargs)
```

---

## Step 1 — Generate raw parquet

```bash
python3 generate_parquet.py \
  --files 4 \
  --mb-per-file 3 \
  --start-id 5000
```

| Flag | Default | Description |
|------|---------|-------------|
| `--files` | `5` | Number of parquet files to upload |
| `--mb-per-file` | `3.0` | Target size per file (MB) |
| `--start-id` | `1000` | Starting `customer_id` |
| `--profile` | — | AWS profile (optional) |

**This example:** 4 files × ~3 MB ≈ **12 MB** → one 10 MB manifest batch.

Output lands at:

```
s3://YOUR_BUCKET/lab2/raw/customers/
```

---

## Step 2 — Run the pipeline

```bash
python3 run_pipeline.py --loop
```

| Flag | Description |
|------|-------------|
| `--loop` | Process all batches until raw prefix is drained |
| `--profile` | AWS profile (optional) |

---

## Full end-to-end

```bash
cd lab2
cp .env.example .env          # edit with your values
export $(grep -v '^#' .env | xargs)
aws sso login --profile YOUR_PROFILE

python3 generate_parquet.py --files 4 --mb-per-file 3 --start-id 5000
python3 run_pipeline.py --loop
```

---

## What happens

```
raw parquet (S3)
      │
      ▼  buffer until ≥ 10 MB, skip archived files
 pending manifest (.pending)
      │
      ▼  read_parquet → dedupe → MERGE INTO
 s3_tables_db.lab2.customers  (Iceberg on S3 Tables)
      │
      ▼  tag archived + delete manifest
 done
```

---

## Troubleshooting

| Message | Fix |
|---------|-----|
| `Missing required environment variables` | Set `LAB2_BUCKET` and `LAB2_S3_TABLES_ARN` in `.env` |
| `AWS SSO token expired` | `aws sso login --profile YOUR_PROFILE` |
| `waiting for 10 MB before creating manifest` | Generate more files or lower `LAB2_BUFFER_MB` |
