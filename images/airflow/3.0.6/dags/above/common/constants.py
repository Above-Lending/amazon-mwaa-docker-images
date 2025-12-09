import os

from airflow.configuration import conf
from airflow.models import Variable


def get_environment_flag() -> str:
    """Get environment flag from Airflow Variable (lazy loaded)."""
    return Variable.get("environment_flag", default_var="staging")


# Cache the value after first call to avoid repeated Variable.get() calls
_ENVIRONMENT_FLAG_CACHE = None


def _get_cached_environment_flag() -> str:
    """Get environment flag with caching."""
    global _ENVIRONMENT_FLAG_CACHE
    if _ENVIRONMENT_FLAG_CACHE is None:
        _ENVIRONMENT_FLAG_CACHE = get_environment_flag()
    return _ENVIRONMENT_FLAG_CACHE


# For backwards compatibility, use environment variable or defer to runtime
ENVIRONMENT_FLAG = os.getenv("ENVIRONMENT_FLAG", "staging")

# DAGS_FOLDER = conf["core"]["dags_folder"] # KeyError with 2.10.1
DAGS_FOLDER = "/usr/local/airflow/dags"
SQL_FOLDER = os.path.join(DAGS_FOLDER, "swat_alerts/sql")

SNOWFLAKE_SWAT_VIEWS = "ALERTS"
SWAT_YAML_FOLDER = os.path.join(DAGS_FOLDER, "swat_alerts/yaml")


def get_s3_conn_id() -> None:
    """Get S3 connection ID."""
    return None


def get_snowflake_conn_id() -> str:
    """Get Snowflake connection ID based on environment."""
    env_flag = _get_cached_environment_flag()
    return (
        "snowflake_prod_connection"
        if env_flag == "prod"
        else "snowflake_staging_connection"
    )


S3_CONN_ID = None
SNOWFLAKE_CONN_ID = None  # Will be set at runtime, use get_snowflake_conn_id()
TALKDESK_ACCOUNT_NAME = "abovelending"

TABLEAU_SITE_ID = "abovelendinginc"
TABLEAU_BACKUP_BUCKET_DIRECTORY = "tableau_backup"
TABLEAU_SNAPSHOT_BUCKET_DIRECTORY = "tableau_snapshots"
TABLEAU_CUSTOM_QUERY_BUCKET_DIRECTORY = "custom_queries"
TABLEAU_SERVER_URL = "https://prod-useast-b.online.tableau.com"

DATALAKE_ERROR_DIR = "error"
DATALAKE_LOADED_DIR = "loaded"
DATALAKE_PREPROCESSED_DIR = "preprocessed"
DATALAKE_SOURCE_DIR = "source"
DATALAKE_SOURCE_ARCHIVE_DIR = "source_archive"
DATALAKE_SUCCESS_DIR = "success"


def get_s3_datalake_bucket() -> str:
    """Get S3 datalake bucket based on environment."""
    env_flag = _get_cached_environment_flag()
    return {
        "prod": "prod-datalake-internal",
        "staging": "stage-datalake-internal",
    }[env_flag]


def get_s3_dataengineering_bucket() -> str:
    """Get S3 data engineering bucket based on environment."""
    env_flag = _get_cached_environment_flag()
    return {
        "prod": "above-snowflake",
        "staging": "above-snowflake",
    }[env_flag]


# For backwards compatibility - these will be empty strings at parse time
S3_DATALAKE_BUCKET = ""  # Use get_s3_datalake_bucket() at runtime
S3_DATAENGINEERING_BUCKET = ""  # Use get_s3_dataengineering_bucket() at runtime


def get_db_config() -> dict:
    """Get database configuration based on environment."""
    env_flag = _get_cached_environment_flag()
    _db_lookup = {
        "staging": {
            "RAW_DATABASE_NAME": "ABOVE_DW_STG",
            "CURATED_DATABASE_NAME": "CURATED_STG",
            "TRUSTED_DATABASE_NAME": "TRUSTED_DEV",
            "STORAGE_INTEGRATION_NAME": "ABOVELENDING_DATALAKE_STAGE",
            "UTILS_DATABASE_NAME": "UTILS_STG",  # NEEDS SETUP, does not exist yet
        },
        "prod": {
            "RAW_DATABASE_NAME": "ABOVE_DW_PROD",
            "REFINED_DATABASE_NAME": "REFINED_PROD",
            "CURATED_DATABASE_NAME": "CURATED_PROD",
            "TRUSTED_DATABASE_NAME": "TRUSTED",
            "STORAGE_INTEGRATION_NAME": "ABOVELENDING_DATALAKE_PROD",
            "UTILS_DATABASE_NAME": "UTILS_PROD",  # NEEDS SETUP, does not exist yet
        },
    }
    return _db_lookup[env_flag]


# For backwards compatibility - these will be empty strings at parse time
# Use get_db_config() at runtime instead
RAW_DATABASE_NAME = ""
CURATED_DATABASE_NAME = ""
TRUSTED_DATABASE_NAME = ""
STORAGE_INTEGRATION_NAME = ""
UTILS_DATABASE_NAME = ""
