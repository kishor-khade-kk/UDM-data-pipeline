from pathlib import Path

from dagster import Definitions, definitions, load_from_defs_folder
from dagster_snowflake import SnowflakeResource

from udm_data_project.config import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ROLE,
    SNOWFLAKE_USER,
    SNOWFLAKE_WAREHOUSE,
)


@definitions
def defs():
    loaded = load_from_defs_folder(path_within_project=Path(__file__).parent)
    return Definitions.merge(
        loaded,
        Definitions(
            resources={
                "snowflake": SnowflakeResource(
                    account=SNOWFLAKE_ACCOUNT,
                    user=SNOWFLAKE_USER,
                    password=SNOWFLAKE_PASSWORD,
                    role=SNOWFLAKE_ROLE,
                    warehouse=SNOWFLAKE_WAREHOUSE,
                    database=SNOWFLAKE_DATABASE,
                )
            }
        ),
    )
