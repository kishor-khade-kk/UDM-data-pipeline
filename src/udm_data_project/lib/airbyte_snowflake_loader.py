from typing import Optional

from dagster import AssetExecutionContext
from dagster_snowflake import SnowflakeResource


def verify_airbyte_snowflake_table(
    snowflake: SnowflakeResource,
    table_name: str,
    schema: str,
    context: Optional[AssetExecutionContext] = None,
) -> int:
    """Verify that an Airbyte-loaded table exists in Snowflake and return its row count."""
    full_table = f"{schema}.{table_name.upper()}"

    with snowflake.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {full_table}")
        row_count = cur.fetchone()[0]

    if context:
        context.log.info(f"  {table_name}: {row_count:,} rows found in {full_table}")

    return row_count
