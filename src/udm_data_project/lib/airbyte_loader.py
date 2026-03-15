import subprocess
import tempfile
from pathlib import Path
from typing import Optional

from dagster import AssetExecutionContext
from dagster_snowflake import SnowflakeResource


def copy_csv_from_docker(
    container_name: str,
    container_path: str,
    context: Optional[AssetExecutionContext] = None,
) -> Path:
    """Copy a CSV file from a Docker container to a local temp file and return its path."""
    tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
    tmp_path = Path(tmp.name)
    tmp.close()

    if context:
        context.log.info(f"Copying {container_path} from container {container_name} ...")

    subprocess.run(
        ["docker", "cp", f"{container_name}:{container_path}", str(tmp_path)],
        check=True,
    )
    return tmp_path


def push_airbyte_csv_to_snowflake(
    snowflake: SnowflakeResource,
    csv_path: Path,
    table_name: str,
    schema: str,
    if_exists: str = "replace",
    context: Optional[AssetExecutionContext] = None,
) -> None:
    """Upload a local Airbyte CSV file to Snowflake using PUT + COPY INTO."""
    full_table = f"{schema}.{table_name.upper()}"
    stage_dir = f"@~/{table_name.lower()}/"

    with snowflake.get_connection() as conn:
        cur = conn.cursor()

        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(f"USE SCHEMA {schema}")
        cur.execute("""
            CREATE TEMP FILE FORMAT IF NOT EXISTS tmp_csv_fmt
            TYPE = CSV
            FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
            PARSE_HEADER = TRUE
            REPLACE_INVALID_CHARACTERS = TRUE
        """)

        if context:
            context.log.info(f"  {table_name}: staging file ...")
        cur.execute(f"PUT file://{csv_path} {stage_dir} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

        if if_exists == "replace":
            cur.execute(f"DROP TABLE IF EXISTS {full_table}")

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {full_table}
            USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(
                    INFER_SCHEMA(
                        LOCATION => '{stage_dir}',
                        FILE_FORMAT => 'tmp_csv_fmt'
                    )
                )
            )
        """)

        if context:
            context.log.info(f"  {table_name}: loading into table ...")
        cur.execute(f"""
            COPY INTO {full_table}
            FROM {stage_dir}
            FILE_FORMAT = (FORMAT_NAME = 'tmp_csv_fmt')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PURGE = TRUE
        """)
