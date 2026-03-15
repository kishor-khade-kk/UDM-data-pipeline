# udm_data_project

A Dagster pipeline that loads data from multiple sources (ZIP archives and Airbyte connections) into Snowflake using native PUT + COPY INTO — no pandas or in-memory data processing, making it suitable for large files on memory-constrained machines.

## Project Structure

```
gatekeeper_systems_data/
├── NAV_tables.zip              # Microsoft Dynamics NAV export (place here)
├── SAFEGRAPH_tables.zip        # SafeGraph export (place here)
└── udm_data_project/           # project root
    ├── .env                    # Snowflake credentials (see setup below)
    ├── pyproject.toml          # project dependencies and config
    └── src/udm_data_project/
        ├── config.py           # central config: table lists, schema names, Airbyte connections
        ├── definitions.py      # Dagster definitions entry point + Snowflake resource
        ├── defs/
        │   ├── nav_data_loader.py        # Dagster asset: loads NAV tables → Snowflake BRONZE
        │   ├── safegraph_data_loader.py  # Dagster asset: loads SafeGraph tables → Snowflake BRONZE
        │   └── mappings_data_loader.py   # Dagster asset: triggers Airbyte sync for mappings
        └── lib/
            ├── zip_loader.py             # Streams parquet files out of a ZIP to disk
            ├── snowflake_loader.py       # Uploads parquet to Snowflake via PUT + COPY INTO
            ├── airbyte_loader.py         # Uploads Airbyte local CSV files to Snowflake
            ├── airbyte_snowflake_loader.py  # Verifies Airbyte-loaded tables in Snowflake
            └── airbyte_trigger.py        # Triggers and polls Airbyte connection syncs
```

## Prerequisites

- Python 3.10–3.14
- [`uv`](https://docs.astral.sh/uv/getting-started/installation/)
- A Snowflake account with a warehouse and database already created
- A running Airbyte instance (for Airbyte-based assets)

## Setup

### 1. Install dependencies

```bash
uv sync
```

### 2. Configure Snowflake credentials

Create a `.env` file in the project root:

```env
SNOWFLAKE_ACCOUNT="your-account-identifier"
SNOWFLAKE_USER="your-username"
SNOWFLAKE_PASSWORD='your-password'
SNOWFLAKE_ROLE="your-role"
SNOWFLAKE_WAREHOUSE="your-warehouse"
SNOWFLAKE_DATABASE="your-database"
SNOWFLAKE_SCHEMA="PUBLIC"
```

> If your password contains special characters like `$`, use single quotes to prevent variable interpolation.

### 3. Place data files

Put the following ZIP files in the **parent directory** of the project (i.e. `gatekeeper_systems_data/`):

```
gatekeeper_systems_data/
├── NAV_tables.zip
├── SAFEGRAPH_tables.zip
└── udm_data_project/   ← project root
```

- `NAV_tables.zip` — must contain: `sales_invoice_line.parquet`, `sales_cr_memo_line.parquet`, `general_posting_setup.parquet`, `customer.parquet`
- `SAFEGRAPH_tables.zip` — must contain: `core_poi.parquet`, `brand_info.parquet`

### 4. Configuration reference (`config.py`)

All project-level settings live in `src/udm_data_project/config.py`:

| Setting | Description |
| --- | --- |
| `DATA_DIR` | Parent directory of the project — where the ZIP files are expected |
| `NAV_ZIP` | Path to `NAV_tables.zip` |
| `NAV_TABLES` | List of table names to extract from the NAV ZIP |
| `SAFEGRAPH_ZIP` | Path to `SAFEGRAPH_tables.zip` |
| `SAFEGRAPH_TABLES` | List of table names to extract from the SafeGraph ZIP |
| `SCHEMA_BRONZE` | Raw ingestion layer — `BRONZE` |
| `SCHEMA_STAGING` | Cleaned/transformed layer — `STAGING` |
| `SCHEMA_MART` | Aggregated/business layer — `MART` |
| `SCHEMA_UAT` | User acceptance testing — `UAT` |
| `SCHEMA_PROD` | Production — `PROD` |
| `AIRBYTE_URL` | Base URL of the Airbyte instance (e.g. `http://localhost:8000`) |
| `AIRBYTE_CONNECTIONS` | Dict mapping connection names to Airbyte connection IDs |

To add a new Airbyte connection, add an entry to `AIRBYTE_CONNECTIONS` in `config.py` and create a new asset in `defs/`.

## Running

Start the Dagster UI:

```bash
dg dev
```

Open [http://localhost:3000](http://localhost:3000) in your browser, then materialize the assets. Schemas (`BRONZE`, `STAGING`, etc.) are created automatically on first run.

## How data loading works

### ZIP-based assets (NAV, SafeGraph)

1. Each parquet file is streamed out of the ZIP to a temp file on disk (no in-memory loading)
2. The temp file is uploaded to Snowflake's internal stage via `PUT`
3. Snowflake infers the table schema from the parquet metadata (`INFER_SCHEMA`)
4. Data is loaded server-side via `COPY INTO` and the staged file is purged

### Airbyte-based assets (Mappings)

1. The asset triggers an Airbyte connection sync via the Airbyte REST API
2. It polls the job status until the sync completes or fails
3. Airbyte loads the data directly into Snowflake BRONZE

This approach uses minimal RAM regardless of file size.
