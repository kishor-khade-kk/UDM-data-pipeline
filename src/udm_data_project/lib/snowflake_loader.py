from pathlib import Path
from typing import Optional

from dagster import AssetExecutionContext
from dagster_snowflake import SnowflakeResource


def push_parquet_to_snowflake(
    snowflake: SnowflakeResource,
    parquet_path: Path,
    table_name: str,
    schema: str,
    if_exists: str = "replace",
    context: Optional[AssetExecutionContext] = None,
) -> None:
    """Upload a parquet file to Snowflake using PUT + COPY INTO.

    No pandas or polars involved — Snowflake reads the parquet directly,
    so memory usage is minimal regardless of file size.
    """
    full_table = f"{schema}.{table_name.upper()}"
    stage_dir = f"@~/{table_name.lower()}/"

    with snowflake.get_connection() as conn:
        cur = conn.cursor()

        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(f"USE SCHEMA {schema}")
        cur.execute("CREATE TEMP FILE FORMAT IF NOT EXISTS tmp_parquet_fmt TYPE = PARQUET REPLACE_INVALID_CHARACTERS = TRUE")

        if context:
            context.log.info(f"  {table_name}: staging file ...")
        cur.execute(f"PUT file://{parquet_path} {stage_dir} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

        if if_exists == "replace":
            cur.execute(f"DROP TABLE IF EXISTS {full_table}")

        # Infer schema from the parquet and create the table.
        # BINARY columns (e.g. NAV GUIDs) are remapped to TEXT to avoid cast errors.
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {full_table}
            USING TEMPLATE (
                SELECT ARRAY_AGG(
                    IFF(TYPE = 'BINARY',
                        OBJECT_CONSTRUCT('COLUMN_NAME', COLUMN_NAME, 'TYPE', 'TEXT', 'NULLABLE', NULLABLE),
                        OBJECT_CONSTRUCT(*)
                    )
                )
                FROM TABLE(
                    INFER_SCHEMA(
                        LOCATION => '{stage_dir}',
                        FILE_FORMAT => 'tmp_parquet_fmt'
                    )
                )
            )
        """)

        if context:
            context.log.info(f"  {table_name}: loading into table ...")
        cur.execute(f"""
            COPY INTO {full_table}
            FROM {stage_dir}
            FILE_FORMAT = (FORMAT_NAME = 'tmp_parquet_fmt')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PURGE = TRUE
        """)
