from dagster import AssetExecutionContext, asset
from dagster_snowflake import SnowflakeResource

from udm_data_project.config import NAV_TABLES, NAV_ZIP, SCHEMA_BRONZE
from udm_data_project.lib.snowflake_loader import push_parquet_to_snowflake
from udm_data_project.lib.zip_loader import iter_parquets_from_zip


@asset
def nav_data_loader(context: AssetExecutionContext, snowflake: SnowflakeResource) -> None:
    for i, (table_name, parquet_path) in enumerate(iter_parquets_from_zip(NAV_ZIP, NAV_TABLES), 1):
        context.log.info(f"[{i}/{len(NAV_TABLES)}] Loading {table_name} ...")
        push_parquet_to_snowflake(snowflake, parquet_path, table_name, schema=SCHEMA_BRONZE, context=context)
        context.log.info(f"[{i}/{len(NAV_TABLES)}] Done: {table_name}")
