from dagster import AssetExecutionContext, asset
from dagster_snowflake import SnowflakeResource

from udm_data_project.config import AIRBYTE_CONNECTIONS, AIRBYTE_URL
from udm_data_project.lib.airbyte_trigger import trigger_airbyte_sync


@asset
def mappings_data_loader(context: AssetExecutionContext, snowflake: SnowflakeResource) -> None:
    trigger_airbyte_sync(
        connection_id=AIRBYTE_CONNECTIONS["mappings"],
        airbyte_url=AIRBYTE_URL,
        context=context,
    )
