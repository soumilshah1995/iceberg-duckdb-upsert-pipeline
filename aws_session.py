"""Shared boto3 session helper for lab2."""

from __future__ import annotations

import boto3
from botocore.exceptions import TokenRetrievalError

from config import AWS_REGION, resolve_profile


def boto3_session(profile: str | None = None) -> boto3.Session:
    name = resolve_profile(profile)
    if name:
        print(f"Using AWS profile: {name} (region={AWS_REGION})")
        return boto3.Session(profile_name=name, region_name=AWS_REGION)
    print(f"Using AWS default credential chain (region={AWS_REGION})")
    return boto3.Session(region_name=AWS_REGION)


def s3_client(profile: str | None = None):
    session = boto3_session(profile)
    try:
        # Force credential refresh now so SSO expiry fails with a clear message
        session.client("sts").get_caller_identity()
    except TokenRetrievalError as exc:
        name = resolve_profile(profile)
        raise SystemExit(
            f"AWS SSO token expired for profile '{name}'.\n"
            f"Run: aws sso login --profile {name}"
        ) from exc
    return session.client("s3")
