"""SQL client handling.

This includes RedshiftStream and RedshiftConnector.
"""

from __future__ import annotations

import json
import os
import tempfile
import typing as t
import uuid
from pathlib import Path
from typing import cast

import boto3
import redshift_connector
from sqlalchemy.engine import URL
import sqlalchemy  # noqa: TC002
from botocore.exceptions import NoCredentialsError
from singer_sdk import SQLConnector, SQLStream, SQLTap
from singer_sdk._singerlib.catalog import CatalogEntry
from singer_sdk.exceptions import ConfigValidationError

from tap_redshift.schema import RedshiftSQLToJSONSchema


class RedshiftConnector(SQLConnector):
    """Connects to the Redshift SQL source using redshift_connector."""

    sql_to_jsonschema_converter = RedshiftSQLToJSONSchema

    def __init__(self, config: dict | None = None, sqlalchemy_url: str | None = None):
        """Initialize the Redshift connector.

        Args:
            config: Configuration dictionary
            sqlalchemy_url: SQLAlchemy URL (optional)
        """
        super().__init__(config, sqlalchemy_url)
        self._connection = None
        self._s3_client = None

    def get_credentials(self) -> tuple[str, str]:
        """Use boto3 to get temporary cluster credentials.

        Returns:
        -------
        tuple[str, str]
            username and password
        """
        if self.config.get("use_iam_authentication"):
            client = boto3.client("redshift", region_name=self.config["aws_region"])
            response = client.get_cluster_credentials(
                DbUser=self.config["username"],
                DbName=self.config["database"],
                ClusterIdentifier=self.config["cluster_identifier"],
                DurationSeconds=3600,
                AutoCreate=False,
            )
            return response["DbUser"], response["DbPassword"]
        return self.config["user"], self.config["password"]

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Concatenate a SQLAlchemy URL for use in connecting to the source.

        Args:
            config: A dict with connection parameters

        Returns:
            SQLAlchemy connection string
        """
        # Build Redshift connection string
        host = config.get("host")
        port = config.get("port", 5439)
        database = config.get("database")
        user, password = self.get_credentials()

        # Handle authentication
        sqlalchemy_url = URL.create(
            drivername="redshift+redshift_connector",
            username=user,
            password=password,
            host=host,
            port=port,
            database=database,
            query={"sslmode": "require"},
        )
        return cast(str, sqlalchemy_url)


class RedshiftStream(SQLStream):
    """Stream class for Redshift streams with S3 unload optimization."""

    connector_class = RedshiftConnector

    def __init__(
        self,
        tap: SQLTap,
        catalog_entry: CatalogEntry,
        connector: RedshiftConnector | None = None,
    ) -> None:
        """Initialize the stream.

        Args:
            tap: The parent tap object
            catalog_entry: Catalog entry for this stream
            connector: Optional connector instance
        """
        super().__init__(tap, catalog_entry, connector)
        self._s3_bucket = tap.config.get("s3_bucket")
        self._s3_key_prefix = tap.config.get("s3_key_prefix", "redshift-unload")

    def get_records(self, context) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Args:
            partition: If provided, will read specifically from this data slice.

        Yields:
            One dict per record.
        """
        yield from self._get_records_via_s3_unload()

    def _get_records_via_s3_unload(self) -> t.Iterable[dict[str, t.Any]]:
        """Get records by unloading to S3 first, then downloading and reading.

        Args:
            partition: If provided, will read specifically from this data slice.

        Yields:
            One dict per record.
        """
        # Generate unique S3 path
        unload_id = str(uuid.uuid4())
        s3_path = f"s3://{self._s3_bucket}/{self._s3_key_prefix}/{self.name}/{unload_id}/"

        # Build query
        query = self.get_query()

        try:
            # Unload to S3
            self.unload_to_s3(query, s3_path)

            # Download from S3
            with tempfile.TemporaryDirectory() as temp_dir:
                local_files = self.download_from_s3(s3_path, temp_dir)

                # Read downloaded files
                for file_path in local_files:
                    yield from self._read_unloaded_file(file_path)

        finally:
            # Clean up S3 files
            self._cleanup_s3_files(s3_path)

    @property
    def s3_client(self):
        """Get or create S3 client."""
        if self._s3_client is None:
            if self.config.get("aws_region"):
                self._s3_client = boto3.client("s3", region_name=self.config.get("aws_region"))
            else:
                self._s3_client = boto3.client("s3")
        return self._s3_client

    def unload_to_s3(self, query: str, s3_path: str, unload_options: dict | None = None) -> str:
        """Unload query results to S3 using Redshift UNLOAD command.

        Args:
            query: SQL query to unload
            s3_path: S3 path for unload (s3://bucket/prefix)
            unload_options: Additional UNLOAD options

        Returns:
            S3 path where data was unloaded
        """
        if unload_options is None:
            unload_options = {}

        # Default unload options
        default_options = {
            "DELIMITER": "\\t",
            "NULL AS": "\\\\N",
            "ESCAPE": True,
            "GZIP": True,
            "ALLOWOVERWRITE": True,
            "PARALLEL": "ON",
        }

        # Merge options
        options = {**default_options, **unload_options}

        # Build options string
        options_str = []
        for key, value in options.items():
            if value is True:
                options_str.append(key)
            elif value is False:
                continue
            else:
                options_str.append(f"{key} '{value}'")

        options_clause = " ".join(options_str)

        # Build UNLOAD command
        credentials_clause = f"IAM_ROLE {self.config.get('copy_role_arn')}"

        unload_sql = f"""
        UNLOAD ('{query.replace("'", "''")}')
        TO '{s3_path}'
        {credentials_clause}
        {options_clause}
        """

        # Execute UNLOAD
        with self.connector._engine.connect() as conn:
            conn.execute(sqlalchemy.text(unload_sql))

        return s3_path

    def download_from_s3(self, s3_path: str, local_dir: str) -> list[str]:
        """Download files from S3 to local directory.

        Args:
            s3_path: S3 path (s3://bucket/prefix)
            local_dir: Local directory to download to

        Returns:
            List of local file paths
        """
        # Parse S3 path
        if not s3_path.startswith("s3://"):
            raise ValueError(f"Invalid S3 path: {s3_path}")

        s3_path = s3_path[5:]  # Remove s3://
        bucket, prefix = s3_path.split("/", 1)

        # List objects
        response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if "Contents" not in response:
            return []

        local_files = []
        os.makedirs(local_dir, exist_ok=True)

        for obj in response["Contents"]:
            key = obj["Key"]
            filename = os.path.basename(key)
            local_path = os.path.join(local_dir, filename)

            # Download file
            self.s3_client.download_file(bucket, key, local_path)
            local_files.append(local_path)

        return local_files

    def _read_unloaded_file(self, file_path: str) -> t.Iterable[dict[str, t.Any]]:
        """Read records from an unloaded file.

        Args:
            file_path: Path to the unloaded file

        Yields:
            One dict per record.
        """
        import csv
        import gzip

        # Determine if file is gzipped
        open_func = gzip.open if file_path.endswith(".gz") else open

        # Get column names from schema
        columns = list(self.schema["properties"].keys())

        with open_func(file_path, "rt", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter="\t")
            for row in reader:
                # Handle NULL values
                processed_row = []
                for value in row:
                    if value == "\\N":  # Redshift NULL representation
                        processed_row.append(None)
                    else:
                        processed_row.append(value)

                # Create record dict
                if len(processed_row) == len(columns):
                    record = dict(zip(columns, processed_row))
                    yield record

    def _cleanup_s3_files(self, s3_path: str) -> None:
        """Clean up S3 files after processing.

        Args:
            s3_path: S3 path to clean up
        """
        try:
            # Parse S3 path
            s3_path = s3_path[5:]  # Remove s3://
            bucket, prefix = s3_path.split("/", 1)

            # List and delete objects
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

            if "Contents" in response:
                objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
                if objects_to_delete:
                    self.s3_client.delete_objects(Bucket=bucket, Delete={"Objects": objects_to_delete})
        except Exception as e:
            self.logger.warning(f"Failed to cleanup S3 files: {e}")

    def get_query(self) -> str:
        """Get the SQL query for this stream.

        Args:
            partition: Optional partition info

        Returns:
            SQL query string
        """
        # Build base query
        selected_columns = [f'"{prop_name}"' for prop_name in self.schema["properties"]]
        query = f"SELECT {', '.join(selected_columns)} FROM {self.fully_qualified_name}"

        return query
