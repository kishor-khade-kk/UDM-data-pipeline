from dagster import AssetExecutionContext, asset
from dagster_snowflake import SnowflakeResource

from udm_data_project.config import (
    AIRBYTE_CLIENT_ID,
    AIRBYTE_CLIENT_SECRET,
    AIRBYTE_CONNECTIONS,
    AIRBYTE_CONTAINER_CSV_DIR,
    AIRBYTE_CONTAINER_NAME,
    AIRBYTE_MAPPINGS_TABLES,
    AIRBYTE_URL,
    SCHEMA_BRONZE,
)
from udm_data_project.lib.airbyte_loader import copy_csv_from_docker, push_airbyte_csv_to_snowflake
from udm_data_project.lib.airbyte_trigger import trigger_airbyte_sync


@asset
def mappings_data_loader(context: AssetExecutionContext, snowflake: SnowflakeResource) -> None:
    trigger_airbyte_sync(
        connection_id=AIRBYTE_CONNECTIONS["mappings"],
        airbyte_url=AIRBYTE_URL,
        client_id=AIRBYTE_CLIENT_ID,
        client_secret=AIRBYTE_CLIENT_SECRET,
        context=context,
    )

    for i, table_name in enumerate(AIRBYTE_MAPPINGS_TABLES, 1):
        context.log.info(f"[{i}/{len(AIRBYTE_MAPPINGS_TABLES)}] Loading {table_name} ...")
        container_file = f"{AIRBYTE_CONTAINER_CSV_DIR}/_airbyte_raw_{table_name}.csv"
        csv_path = copy_csv_from_docker(AIRBYTE_CONTAINER_NAME, container_file, context=context)
        try:
            push_airbyte_csv_to_snowflake(snowflake, csv_path, table_name, schema=SCHEMA_BRONZE, context=context)
        finally:
            csv_path.unlink(missing_ok=True)
        context.log.info(f"[{i}/{len(AIRBYTE_MAPPINGS_TABLES)}] Done: {table_name}")
