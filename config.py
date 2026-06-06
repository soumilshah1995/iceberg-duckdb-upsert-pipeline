import os


def resolve_profile(explicit: str | None = None) -> str | None:
    """
    Profile order: --profile CLI flag > LAB2_AWS_PROFILE > AWS_PROFILE > None (boto3 default chain).
    """
    if explicit:
        return explicit
    lab = os.getenv("LAB2_AWS_PROFILE")
    if lab and lab.strip():
        return lab.strip()
    env = os.getenv("AWS_PROFILE")
    if env and env.strip():
        return env.strip()
    return None


AWS_PROFILE = resolve_profile()
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Set via env — see .env.example (no hardcoded bucket or account defaults)
BUCKET = os.getenv("LAB2_BUCKET", "")
URI_SCHEME = "s3://"

RAW_PREFIX = os.getenv("LAB2_RAW_PREFIX", "lab2/raw/customers/")
PENDING_PREFIX = os.getenv("LAB2_PENDING_PREFIX", "lab2/manifests/pending/")
ERROR_PREFIX = os.getenv("LAB2_ERROR_PREFIX", "lab2/manifests/error/")

BUFFER_BYTES = int(os.getenv("LAB2_BUFFER_MB", "10")) * 1024 * 1024

ARCHIVED_TAG_KEY = "lab2_status"
ARCHIVED_TAG_VALUE = "archived"

S3_TABLES_ARN = os.getenv("LAB2_S3_TABLES_ARN", "")
ICEBERG_SCHEMA = os.getenv("LAB2_ICEBERG_SCHEMA", "lab2")
ICEBERG_TABLE = os.getenv("LAB2_ICEBERG_TABLE", "customers")
CATALOG_ALIAS = "s3_tables_db"


def validate_config() -> None:
    """Fail fast with a clear message if required env vars are missing."""
    missing = []
    if not BUCKET:
        missing.append("LAB2_BUCKET")
    if not S3_TABLES_ARN:
        missing.append("LAB2_S3_TABLES_ARN")
    if missing:
        raise SystemExit(
            "Missing required environment variables: "
            + ", ".join(missing)
            + "\nCopy .env.example to .env and fill in your values."
        )
