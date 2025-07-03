# tap-redshift

`tap-redshift` is a Singer tap for Amazon Redshift, built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Features

- **Multiple Authentication Methods**: Support for username/password and IAM authentication
- **Redshift Serverless Support**: Full support for Redshift Serverless workgroups
- **S3 UNLOAD Optimization**: Uses Redshift's UNLOAD command to S3 for better performance on large datasets
- **Comprehensive Type Mapping**: Complete mapping from Redshift SQL types to JSON Schema types
- **SSL Support**: Secure connections with configurable SSL modes

## Installation

Install from GitHub:

```bash
pipx install git+https://github.com/tobiascadee/tap-redshift.git@main
```

## Configuration

### Authentication Options

#### Username/Password Authentication
```json
{
  "host": "your-cluster.abc123.us-west-2.redshift.amazonaws.com",
  "port": "5439",
  "database": "your_database",
  "schema": "public",
  "user": "your_username",
  "password": "your_password",
  "use_iam_authentication": false
}
```

#### IAM Authentication (Provisioned Cluster)
```json
{
  "host": "your-cluster.abc123.us-west-2.redshift.amazonaws.com",
  "port": 5439,
  "database": "your_database",
  "schema": "public",
  "use_iam_authentication": true,
  "db_user": "your_db_user",
  "cluster_identifier": "your-cluster",
  "aws_region": "us-west-2",
  "aws_access_key_id": "your_access_key",
  "aws_secret_access_key": "your_secret_key"
}
```

#### IAM Authentication (Serverless)
```json
{
  "host": "your-workgroup.123456789.us-west-2.redshift-serverless.amazonaws.com",
  "database": "your_database",
  "schema": "public",
  "use_iam_authentication": true,
  "is_serverless": true,
  "serverless_work_group": "your-workgroup",
  "aws_region": "us-west-2"
}
```

### S3 UNLOAD Configuration (Recommended)

For improved performance on large datasets, configure S3 UNLOAD:

```json
{
  "s3_bucket": "your-unload-bucket",
  "s3_key_prefix": "redshift-unload"
}
```

When enabled, the tap will:
1. Use Redshift's UNLOAD command to export data to S3
2. Download the files locally for processing
3. Clean up S3 files after processing

### Required Settings

| Setting | Description |
|---------|-------------|
| `host` | Redshift cluster endpoint |
| `database` | Database name |

### Authentication Settings

| Setting | Description | Required |
|---------|-------------|----------|
| `user` | Username for database auth | If not using IAM |
| `password` | Password for database auth | If not using IAM |
| `use_iam_authentication` | Use IAM instead of username/password | No (default: false) |
| `db_user` | Database user for IAM auth | If using IAM |
| `cluster_identifier` | Cluster identifier for IAM auth | If using IAM with provisioned cluster |
| `serverless_work_group` | Workgroup name | If using Serverless |

### AWS Settings

| Setting | Description | Required |
|---------|-------------|----------|
| `aws_region` | AWS region | If using IAM or S3 |
| `aws_access_key_id` | AWS access key | If not using IAM roles/profiles |
| `aws_secret_access_key` | AWS secret key | If not using IAM roles/profiles |
| `aws_session_token` | AWS session token | For temporary credentials |
| `aws_profile` | AWS profile name | If using AWS profiles |

### Optional Settings

| Setting | Description | Default |
|---------|-------------|---------|
| `port` | Redshift port | 5439 |
| `schema` | Schema to extract from | public |
| `ssl` | Use SSL connection | true |
| `sslmode` | SSL mode | require |
| `is_serverless` | Using Serverless | false |
| `s3_key_prefix` | S3 key prefix for UNLOAD | redshift-unload |
| `dates_as_string` | Convert dates to strings vs date formats | true |
| `super_as_object` | Treat SUPER columns as objects vs strings | false |

### Supported SQL Types

The tap includes a dedicated `RedshiftSQLToJSONSchema` converter class that extends the Singer SDK's base converter with Redshift-specific functionality:

| Redshift Type | JSON Schema Type | Notes |
|---------------|------------------|-------|
| Standard SQL Types | | Handled by base SQLToJSONSchema class |
| SMALLINT, INTEGER, BIGINT | integer | With appropriate min/max values |
| DECIMAL, NUMERIC | number | With precision handling via registered method |
| REAL, FLOAT, DOUBLE PRECISION | number | |
| BOOLEAN | boolean | |
| CHAR, VARCHAR, TEXT | string | |
| DATE, TIMESTAMP, TIME | string | Configurable format via registered methods |
| **Redshift-Specific Types** | | **Custom handled by RedshiftSQLToJSONSchema** |
| SUPER | string or object | Configurable via super_as_object |
| GEOMETRY, GEOGRAPHY | string | Spatial data |
| HLLSKETCH | string | HyperLogLog sketches |

#### Architecture

The converter uses the **Singer SDK standard pattern**:
- **Class attribute**: `RedshiftConnector.sql_to_jsonschema_converter = RedshiftSQLToJSONSchema`
- **Base class handles** standard SQLAlchemy types (integers, strings, booleans, etc.)
- **Registered methods override** date/time types for configurable format handling  
- **String-based dispatch** handles Redshift-specific types from database introspection
- **Automatic instantiation**: The SDK automatically creates the converter instance with tap config

#### Schema Conversion Options

- **`dates_as_string`** (default: `true`): When `true`, date/time columns are converted to plain strings. When `false`, they include proper JSON Schema format constraints (`date`, `date-time`, `time`).

- **`super_as_object`** (default: `false`): When `true`, SUPER columns are treated as flexible objects that can contain JSON-like data. When `false`, they are treated as strings.

Example configuration for structured data handling:
```json
{
  "dates_as_string": false,
  "super_as_object": true
}
```

## Usage

### Direct Execution

```bash
# Test connection
tap-redshift --config config.json --discover > catalog.json

# Extract data
tap-redshift --config config.json --catalog catalog.json
```

### With Meltano

Add to your `meltano.yml`:

```yaml
extractors:
- name: tap-redshift
  namespace: tap_redshift
  pip_url: git+https://github.com/tobiascadee/tap-redshift.git@main
  settings:
  - name: host
  - name: database
  - name: user
  - name: password
    kind: password
    sensitive: true
  # ... other settings
```

Then run:

```bash
meltano install extractor tap-redshift
meltano invoke tap-redshift --discover > catalog.json
meltano run tap-redshift target-jsonl
```

## Performance Recommendations

2. **IAM Authentication**: Use IAM authentication for better security
3. **Appropriate Batch Size**: Adjust `batch_size` based on your data size
4. **Schema Selection**: Specify the exact schema to avoid unnecessary discovery

## Development

### Setup Development Environment

```bash
git clone https://github.com/tobiascadee/tap-redshift.git
cd tap-redshift
uv sync
```

### Run Tests

```bash
uv run pytest
```

### Test CLI

```bash
uv run tap-redshift --help
```

## Dependencies

- `singer-sdk`: Meltano Singer SDK
- `redshift-connector`: Official AWS Redshift connector
- `boto3`: AWS SDK for S3 operations
- `sqlalchemy`: SQL toolkit

## License

Apache 2.0
