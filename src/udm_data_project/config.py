import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Directories
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).parents[3]
OUTPUT_DIR = DATA_DIR / "output"

# ---------------------------------------------------------------------------
# NAV (Microsoft Dynamics NAV)
# ---------------------------------------------------------------------------
NAV_ZIP = DATA_DIR / "NAV_tables.zip"
NAV_TABLES = [
    "sales_invoice_line",
    "sales_cr_memo_line",
    "general_posting_setup",
    "customer",
]

# ---------------------------------------------------------------------------
# SafeGraph
# ---------------------------------------------------------------------------
SAFEGRAPH_ZIP = DATA_DIR / "SAFEGRAPH_tables.zip"
SAFEGRAPH_TABLES = [
    "core_poi",
    "brand_info",
]

# ---------------------------------------------------------------------------
# Snowflake
# ---------------------------------------------------------------------------
SNOWFLAKE_ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ROLE = os.environ["SNOWFLAKE_ROLE"]
SNOWFLAKE_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]
SNOWFLAKE_DATABASE = os.environ["SNOWFLAKE_DATABASE"]

# Schemas
SCHEMA_BRONZE = "BRONZE"
SCHEMA_STAGING = "STAGING"
SCHEMA_MART = "MART"
SCHEMA_UAT = "UAT"
SCHEMA_PROD = "PROD"

# ---------------------------------------------------------------------------
# (Future sources go below, following the same pattern)
# ---------------------------------------------------------------------------
