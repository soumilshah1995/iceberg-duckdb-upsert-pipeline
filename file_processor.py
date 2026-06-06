"""Volume-based manifest creation and S3 file lifecycle for lab2."""

from __future__ import annotations

import time
from typing import Optional
from urllib.parse import urlparse

from botocore.exceptions import TokenRetrievalError

from aws_session import boto3_session
from config import (
    ARCHIVED_TAG_KEY,
    ARCHIVED_TAG_VALUE,
    AWS_REGION,
    BUFFER_BYTES,
    BUCKET,
    ERROR_PREFIX,
    PENDING_PREFIX,
    RAW_PREFIX,
    URI_SCHEME,
    resolve_profile,
)


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    return parsed.netloc, parsed.path.lstrip("/")


class FileProcessor:
    """Build volume-based manifests from raw S3 files and manage lifecycle."""

    def __init__(
        self,
        bucket_name: str = BUCKET,
        raw_prefix: str = RAW_PREFIX,
        pending_prefix: str = PENDING_PREFIX,
        error_prefix: str = ERROR_PREFIX,
        uri_scheme: str = URI_SCHEME,
        buffer_bytes: int = BUFFER_BYTES,
        profile: str | None = None,
        region: str = AWS_REGION,
    ) -> None:
        name = resolve_profile(profile)
        try:
            session = boto3_session(name)
            session.client("sts").get_caller_identity()
        except TokenRetrievalError as exc:
            raise SystemExit(
                f"AWS SSO token expired for profile '{name}'.\n"
                f"Run: aws sso login --profile {name}"
            ) from exc
        self.s3 = session.client("s3")
        self.bucket_name = bucket_name
        self.raw_prefix = raw_prefix
        self.pending_prefix = pending_prefix
        self.error_prefix = error_prefix
        self.uri_scheme = uri_scheme
        self.buffer_bytes = buffer_bytes

    def _is_archived(self, key: str) -> bool:
        """Skip files already tagged as archived."""
        try:
            resp = self.s3.get_object_tagging(Bucket=self.bucket_name, Key=key)
            tags = {t["Key"]: t["Value"] for t in resp.get("TagSet", [])}
            return tags.get(ARCHIVED_TAG_KEY) == ARCHIVED_TAG_VALUE
        except self.s3.exceptions.NoSuchKey:
            return False

    def create_pending_manifest(self) -> Optional[str]:
        """
        Scan raw prefix and buffer file paths until volume >= buffer_bytes.
        Skips files tagged as archived. Returns manifest S3 URI or None.
        """
        paginator = self.s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket_name, Prefix=self.raw_prefix)

        files: list[str] = []
        buffered_bytes = 0

        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("/"):
                    continue
                if self._is_archived(key):
                    continue

                size = obj["Size"]
                files.append(f"{self.uri_scheme}{self.bucket_name}/{key}")
                buffered_bytes += size

                if buffered_bytes >= self.buffer_bytes:
                    break
            if buffered_bytes >= self.buffer_bytes:
                break

        if not files or buffered_bytes < self.buffer_bytes:
            if files:
                mb = buffered_bytes / (1024 * 1024)
                need = self.buffer_bytes / (1024 * 1024)
                print(
                    f"Buffered {len(files)} file(s) ({mb:.2f} MB) — "
                    f"waiting for {need:.0f} MB before creating manifest"
                )
            return None

        manifest_content = "\n".join(files)
        unix_ts = int(time.time())
        manifest_key = f"{self.pending_prefix}{unix_ts}.pending"
        self.s3.put_object(
            Bucket=self.bucket_name,
            Key=manifest_key,
            Body=manifest_content.encode("utf-8"),
        )
        mb = buffered_bytes / (1024 * 1024)
        print(
            f"Created manifest with {len(files)} files "
            f"({mb:.2f} MB): {manifest_key}"
        )
        return f"{self.uri_scheme}{self.bucket_name}/{manifest_key}"

    def archive_file(self, file_uri: str) -> None:
        """Tag a processed raw file so it is skipped on the next scan."""
        _, key = _parse_s3_uri(file_uri)
        self.s3.put_object_tagging(
            Bucket=self.bucket_name,
            Key=key,
            Tagging={
                "TagSet": [
                    {"Key": ARCHIVED_TAG_KEY, "Value": ARCHIVED_TAG_VALUE},
                ]
            },
        )
        print(f"Archived (tagged): {key}")

    def archive_files(self, file_uris: list[str]) -> None:
        for uri in file_uris:
            self.archive_file(uri)

    def delete_manifest(self, manifest_uri: str) -> None:
        """Remove manifest after successful processing."""
        _, key = _parse_s3_uri(manifest_uri)
        self.s3.delete_object(Bucket=self.bucket_name, Key=key)
        print(f"Deleted manifest: {key}")

    def error_manifest(self, manifest_uri: str) -> None:
        """Move manifest to error prefix for inspection/replay."""
        _, src_key = _parse_s3_uri(manifest_uri)
        filename = src_key.rsplit("/", 1)[-1]
        dst_key = f"{self.error_prefix}{filename}"

        self.s3.copy_object(
            Bucket=self.bucket_name,
            CopySource={"Bucket": self.bucket_name, "Key": src_key},
            Key=dst_key,
        )
        self.s3.delete_object(Bucket=self.bucket_name, Key=src_key)
        print(f"Moved manifest to error/: {dst_key}")

    def read_manifest(self, manifest_uri: str) -> list[str]:
        """Load file URIs listed in a manifest."""
        _, key = _parse_s3_uri(manifest_uri)
        resp = self.s3.get_object(Bucket=self.bucket_name, Key=key)
        body = resp["Body"].read().decode("utf-8")
        return [line.strip() for line in body.splitlines() if line.strip()]
