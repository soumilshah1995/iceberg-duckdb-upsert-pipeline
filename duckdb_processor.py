"""DuckDB Iceberg MERGE processor for manifest batches."""

from __future__ import annotations

import duckdb

from config import (
    AWS_REGION,
    CATALOG_ALIAS,
    ICEBERG_SCHEMA,
    ICEBERG_TABLE,
    S3_TABLES_ARN,
    resolve_profile,
)


class DuckDBProcessor:
    """Read parquet files from a manifest and MERGE INTO an Iceberg table."""

    SETUP_SQL = """
        INSTALL aws; LOAD aws;
        INSTALL httpfs; LOAD httpfs;
        INSTALL iceberg; LOAD iceberg;
        INSTALL parquet; LOAD parquet;

        CREATE OR REPLACE SECRET s3_secret (
            TYPE s3,
            PROVIDER credential_chain,
            CHAIN 'config'{profile_clause},
            REGION '{region}'
        );

        ATTACH '{arn}'
        AS {catalog} (
            TYPE iceberg,
            ENDPOINT_TYPE s3_tables
        );

        CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};

        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table} (
            customer_id INTEGER,
            name        VARCHAR,
            city        VARCHAR,
            balance     DOUBLE
        );
    """

    MERGE_SQL = """
        MERGE INTO {catalog}.{schema}.{table} AS target
        USING (
            -- Iceberg rejects MERGE when the same key appears more than once in USING.
            -- Deduplicate by customer_id; latest file wins (filename DESC).
            SELECT customer_id, name, city, balance
            FROM (
                SELECT customer_id, name, city, balance,
                       ROW_NUMBER() OVER (
                           PARTITION BY customer_id
                           ORDER BY filename DESC
                       ) AS rn
                FROM read_parquet({files}, filename=true)
            )
            WHERE rn = 1
        ) AS upserts
        ON target.customer_id = upserts.customer_id
        WHEN MATCHED THEN UPDATE
        WHEN NOT MATCHED THEN INSERT;
    """

    def __init__(
        self,
        profile: str | None = None,
        region: str = AWS_REGION,
        catalog: str = CATALOG_ALIAS,
        schema: str = ICEBERG_SCHEMA,
        table: str = ICEBERG_TABLE,
        s3_tables_arn: str = S3_TABLES_ARN,
    ) -> None:
        self.profile = resolve_profile(profile)
        self.region = region
        self.catalog = catalog
        self.schema = schema
        self.table = table
        self.s3_tables_arn = s3_tables_arn
        self.con = duckdb.connect()

    def setup(self) -> None:
        if self.profile:
            print(f"Using AWS profile: {self.profile} (region={self.region})")
        else:
            print(f"Using AWS default credential chain (region={self.region})")
        profile_clause = f",\n            PROFILE '{self.profile}'" if self.profile else ""
        sql = self.SETUP_SQL.format(
            profile_clause=profile_clause,
            region=self.region,
            arn=self.s3_tables_arn,
            catalog=self.catalog,
            schema=self.schema,
            table=self.table,
        )
        self.con.execute(sql)

    def merge_from_manifest(self, file_uris: list[str]) -> int:
        """Read all parquet files in the manifest and upsert into Iceberg."""
        if not file_uris:
            return 0

        files_literal = "[" + ", ".join(f"'{u}'" for u in file_uris) + "]"
        merge_sql = self.MERGE_SQL.format(
            catalog=self.catalog,
            schema=self.schema,
            table=self.table,
            files=files_literal,
        )
        self.con.execute(merge_sql)
        row_count = self.con.execute(
            f"SELECT COUNT(*) FROM {self.catalog}.{self.schema}.{self.table}"
        ).fetchone()[0]
        print(f"MERGE complete — {self.catalog}.{self.schema}.{self.table} has {row_count} rows")
        return row_count

    def query_table(self, limit: int = 10) -> list:
        return self.con.execute(
            f"SELECT * FROM {self.catalog}.{self.schema}.{self.table} "
            f"ORDER BY customer_id LIMIT {limit}"
        ).fetchall()

    def close(self) -> None:
        self.con.close()
