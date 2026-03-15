from dagster import AssetExecutionContext, asset
from dagster_snowflake import SnowflakeResource


@asset
def mappings_data_loader(context: AssetExecutionContext, snowflake: SnowflakeResource) -> None:
    pass
