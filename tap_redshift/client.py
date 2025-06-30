"""SQL client handling.

This includes RedshiftStream and RedshiftConnector.
"""

from __future__ import annotations

import tempfile
import typing as t
import uuid
from contextlib import contextmanager
from pathlib import Path

import boto3
import pandas as pd
import pyarrow.parquet as pq
import redshift_connector
import singer_sdk.typing as th
from redshift_connector import Cursor
from singer_sdk import SQLStream
from singer_sdk.connectors import SQLConnector
from singer_sdk.singerlib.catalog import CatalogEntry, MetadataMapping
from singer_sdk.singerlib.schema import Schema


class RedshiftConnector(SQLConnector):
    """Connects to Redshift using redshift_connector directly."""

    def __init__(self, config: dict | None = None) -> None:
        """Initialize the Redshift connector.

        Args:
            config: Configuration dictionary
        """
        # Initialize the parent SQLConnector but we'll override key methods
        super().__init__(config or {})
        self._redshift_connection = None
        self._s3_client = None

    @contextmanager
    def connect_cursor(self) -> t.Iterator[Cursor]:
        """Get or create a redshift_connector connection."""
        connection_params = {
            "host": self.config["host"],
            "database": self.config["database"],
            "port": self.config["port"],
            "ssl": self.config["ssl"],
            "sslmode": self.config["sslmode"],
            "iam": self.config["use_iam_authentication"],
            "region": self.config["aws_region"],
            "cluster_identifier": self.config["cluster_identifier"],
            "db_user": self.config["username"],
            "user": self.config["username"],
        }
        with redshift_connector.connect(**connection_params) as conn:
            with conn.cursor() as cursor:
                yield cursor
            conn.commit()

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
        return self.config.get("username", self.config.get("user", "")), self.config.get("password", "")

    def get_table_columns(self, schema_name: str, cursor: Cursor) -> list[dict]:
        """Get column information for a table.

        Args:
            schema_name: Schema name

        Returns:
            List of column dictionaries with table_name, name, type, nullable info
        """
        cursor.execute(f"""
        SELECT
            table_name,
            column_name,
            data_type,
            is_nullable,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            column_default
        FROM information_schema.columns
        WHERE table_schema = '{schema_name}'
        ORDER BY ordinal_position
        """)
        return [
            {
                "table_name": row[0],
                "column_name": row[1],
                "data_type": row[2],
                "is_nullable": row[3] == "YES",
                "character_maximum_length": row[4],
                "numeric_precision": row[5],
                "numeric_scale": row[6],
                "column_default": row[7],
            }
            for row in cursor.fetchall()
        ]

    # Override SQLAlchemy-specific methods since we use redshift_connector directly
    def get_sqlalchemy_url(self, config: dict) -> str:
        """Return a dummy SQLAlchemy URL - not actually used."""
        # This method is required by the parent class but we don't use SQLAlchemy
        return "redshift+redshift_connector://dummy"

    @property
    def sqlalchemy_url(self) -> str:
        """Return a dummy SQLAlchemy URL - not actually used."""
        return self.get_sqlalchemy_url(self.config)

    def create_engine(self):
        """Override to prevent creating a SQLAlchemy engine."""
        # We don't actually use SQLAlchemy, return None
        return None

    @property
    def _engine(self):
        """Override to prevent creating a SQLAlchemy engine."""
        return None

    def get_schema_names(self, cursor: Cursor) -> list[str]:
        """Get available schema names using redshift_connector."""
        # Override parent method to use redshift_connector instead of SQLAlchemy
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_internal')
            ORDER BY schema_name
        """)
        return [row[0] for row in cursor.fetchall()]

    def get_table_names(self, schema_name: str, cursor: Cursor) -> list:
        """Get table names for a schema using redshift_connector."""
        # Override parent method to use redshift_connector instead of SQLAlchemy
        cursor.execute(f"""
            SELECT
                table_name,
                CASE
                    WHEN table_type = 'BASE TABLE'
                        THEN false
                    ELSE true
                END AS is_view
            FROM information_schema.tables
            WHERE table_schema = '{schema_name}'
            ORDER BY table_name
        """)
        return list(cursor.fetchall())

    def get_column_type_helper(self, data_type: str) -> th.JSONTypeHelper:
        """Convert Redshift data type string to appropriate Singer SDK type helper.

        Args:
            data_type: The Redshift data type string (e.g., 'varchar', 'integer', etc.)

        Returns:
            Appropriate Singer SDK type helper object
        """
        # Normalize the data type (remove parameters and convert to lowercase)
        base_type = data_type.lower().split("(")[0].strip()

        # Map Redshift types to Singer SDK type helpers
        type_mapping = {
            # Integer types
            "smallint": th.IntegerType,
            "integer": th.IntegerType,
            "int": th.IntegerType,
            "bigint": th.IntegerType,
            # Numeric types
            "decimal": th.NumberType,
            "numeric": th.NumberType,
            "real": th.NumberType,
            "float": th.NumberType,
            "double precision": th.NumberType,
            "float4": th.NumberType,
            "float8": th.NumberType,
            # String types
            "varchar": th.StringType,
            "char": th.StringType,
            "text": th.StringType,
            "bpchar": th.StringType,
            # Boolean type
            "boolean": th.BooleanType,
            "bool": th.BooleanType,
            # Date/time types
            "date": th.DateType,
            "timestamp": th.DateTimeType,
            "timestamptz": th.DateTimeType,
            "timestamp without time zone": th.DateTimeType,
            "timestamp with time zone": th.DateTimeType,
            "time": th.TimeType,
            "timetz": th.TimeType,
            "time without time zone": th.TimeType,
            "time with time zone": th.TimeType,
            # Redshift-specific types - treat as strings
            "super": th.StringType,
            "geometry": th.StringType,
            "geography": th.StringType,
            "hllsketch": th.StringType,
        }

        # Get the type helper class, defaulting to StringType for unknown types
        type_helper_class = type_mapping.get(base_type, th.StringType)

        # Return an instance of the type helper
        return type_helper_class()

    def discover_catalog_entry(
        self,
        engine=None,  # type: ignore[misc]
        inspected=None,  # type: ignore[misc]
        schema_name: str | None = None,
        table_name: str = "",
        is_view: bool = False,  # noqa: FBT001, FBT002
        *,
        reflected_columns: list[dict] | None = None,
        reflected_pk=None,  # type: ignore[misc]
        reflected_indices: list | None = None,
    ) -> CatalogEntry:
        """Create `CatalogEntry` object for the given table or view.

        This method overrides the base class to properly handle Redshift type conversion.
        """
        # Initialize unique stream name
        unique_stream_id = f"{schema_name}-{table_name}" if schema_name else table_name

        # Backwards-compatibility
        reflected_columns = reflected_columns or []
        reflected_indices = reflected_indices or []

        # Detect key properties - simplified since we don't have primary key info from Redshift queries
        key_properties: list[str] = []

        # Initialize columns list using our type helpers directly
        properties = [
            th.Property(
                name=column["name"],
                wrapped=column["type"],  # This is now already a type helper object
                nullable=column.get("nullable", False),
                required=column["name"] in key_properties,
                description=column.get("comment"),
            )
            for column in reflected_columns
        ]

        properties_list = th.PropertiesList(*properties)
        schema = properties_list.to_dict()

        # Initialize available replication methods
        addl_replication_methods: list[str] = []  # By default an empty list.
        replication_method = next(reversed(["FULL_TABLE", *addl_replication_methods]))

        # Create the catalog entry object
        return CatalogEntry(
            tap_stream_id=unique_stream_id,
            stream=unique_stream_id,
            table=table_name,
            key_properties=key_properties,
            schema=Schema.from_dict(schema),
            is_view=is_view,
            replication_method=replication_method,
            metadata=MetadataMapping.get_standard_metadata(
                schema_name=schema_name,
                schema=schema,
                replication_method=replication_method,
                key_properties=key_properties,
                valid_replication_keys=None,  # Must be defined by user
            ),
            database=None,  # Expects single-database context
            row_count=None,
            stream_alias=None,
            replication_key=None,  # Must be defined by user
        )

    def discover_catalog_entries(self, exclude_schemas=None) -> list[dict]:
        """Discover catalog entries using redshift_connector directly.

        Returns dictionaries instead of CatalogEntry objects for JSON serialization.
        """
        with self.connect_cursor() as cursor:
            result: list[dict] = []

            # Get schema names to discover
            config_schema_name = self.config.get("schemas")
            schema_names = config_schema_name if config_schema_name else self.get_schema_names(cursor)

            # Filter out excluded schemas
            if exclude_schemas:
                schema_names = [s for s in schema_names if s not in exclude_schemas]

            for schema_name in schema_names:
                table_names = self.get_table_names(schema_name, cursor)
                columns = self.get_table_columns(schema_name, cursor)
                for table_name, is_view in table_names:
                    reflected_columns = [
                        {
                            "name": col["column_name"],
                            "type": self.get_column_type_helper(col["data_type"]),
                            "nullable": col["is_nullable"],
                        }
                        for col in columns
                        if col["table_name"] == table_name
                    ]
                    (
                        result.append(
                            self.discover_catalog_entry(
                                engine=None,  # type: ignore[misc]
                                inspected=None,  # type: ignore[misc]
                                schema_name=schema_name,
                                table_name=table_name,
                                is_view=is_view,
                                reflected_columns=reflected_columns,
                            ).to_dict()
                        )
                    )

            return result


class RedshiftStream(SQLStream):
    """Stream class for Redshift streams with S3 unload optimization."""

    connector_class = RedshiftConnector
    _s3_client = None

    def get_records(self, context: t.Mapping[str, t.Any] | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Args:
            context: Stream context (unused in current implementation)

        Yields:
            One dict per record.
        """
        yield from self._get_records_via_s3_unload()

    def _get_records_via_s3_unload(self) -> t.Iterable[dict[str, t.Any]]:
        """Get records by unloading to S3 as Parquet first, then downloading and reading.

        Yields:
            One dict per record.
        """
        # Generate unique S3 path
        unload_id = str(uuid.uuid4())
        if self.config.get("s3_key_prefix"):
            s3_path = f"s3://{self.config['s3_bucket']}/{self.config['s3_key_prefix']}/{self.name}/{unload_id}/"
        else:
            s3_path = f"s3://{self.config['s3_bucket']}/{self.name}/{unload_id}/"

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
            if self._tap.config.get("aws_region"):
                self._s3_client = boto3.client("s3", region_name=self._tap.config.get("aws_region"))
            else:
                self._s3_client = boto3.client("s3")
        return self._s3_client

    def unload_to_s3(self, query: str, s3_path: str, unload_options: dict | None = None) -> str:
        """Unload query results to S3 using Redshift UNLOAD command in Parquet format.

        Args:
            query: SQL query to unload
            s3_path: S3 path for unload (s3://bucket/prefix)
            unload_options: Additional UNLOAD options

        Returns:
            S3 path where data was unloaded
        """
        if unload_options is None:
            unload_options = {}

        # Default unload options for Parquet format
        options = ["FORMAT PARQUET", "ALLOWOVERWRITE"]

        # Build UNLOAD command
        unload_sql = f"""
        UNLOAD ('{query.replace("'", "''")}')
        TO '{s3_path}'
        IAM_ROLE '{self.config["copy_role_arn"]}'
        {" ".join(options)}
        """

        # Execute UNLOAD using direct connection
        with self.connector.connect_cursor() as cursor:
            self.logger.info("Executing UNLOAD command")
            cursor.execute(unload_sql)

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
            msg = f"Invalid S3 path: {s3_path}"
            raise ValueError(msg)

        s3_path = s3_path[5:]  # Remove s3://
        bucket, prefix = s3_path.split("/", 1)

        # List objects
        response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if "Contents" not in response:
            return []

        local_files = []
        Path(local_dir).mkdir(parents=True, exist_ok=True)

        for obj in response["Contents"]:
            key = obj["Key"]
            filename = Path(key).name
            local_path = Path(local_dir) / filename

            # Download file
            self.logger.info("Downloading %s from S3", key)
            self.s3_client.download_file(bucket, key, str(local_path))
            local_files.append(str(local_path))

        return local_files

    def _read_unloaded_file(self, file_path: str) -> t.Iterable[dict[str, t.Any]]:
        """Read records from an unloaded Parquet file.

        Args:
            file_path: Path to the unloaded Parquet file

        Yields:
            One dict per record.
        """
        try:
            # Read Parquet file using pyarrow
            self.logger.info("Reading Parquet file %s", file_path)
            table = pq.read_table(file_path)
            yield from table.to_pylist()

        except (OSError, ValueError, ImportError) as e:
            self.logger.warning("Failed to read Parquet file %s: %s", file_path, e)
            return

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
            self.logger.warning("Failed to cleanup S3 files: %s", e)

    def get_query(self) -> str:
        """Get the SQL query for this stream.

        Returns:
            SQL query string
        """
        # Build base query
        selected_columns = [f'"{prop_name}"' for prop_name in self.schema["properties"]]
        return f"SELECT {', '.join(selected_columns)} FROM {self.fully_qualified_name}"  # noqa: S608
