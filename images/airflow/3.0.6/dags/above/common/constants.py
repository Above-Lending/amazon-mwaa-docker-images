import os

from airflow.configuration import conf
from airflow.sdk import Variable

# Lazy-load environment flag to avoid DB call at import time
def _get_environment_flag() -> str:
    return Variable.get("environment_flag", default="staging")

# DAGS_FOLDER = conf["core"]["dags_folder"] # KeyError with 2.10.1
DAGS_FOLDER = "/usr/local/airflow/dags"
SQL_FOLDER = os.path.join(DAGS_FOLDER, "swat_alerts/sql")

SNOWFLAKE_SWAT_VIEWS = "ALERTS"
SWAT_YAML_FOLDER = os.path.join(DAGS_FOLDER, "swat_alerts/yaml")


S3_CONN_ID = None

# Static constants that don't require DB access
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

# Lazy evaluation to avoid import-time DB calls
def get_snowflake_conn_id():
    env: str = _get_environment_flag()
    return (
        "snowflake_prod_connection_3x"
        if env == "prod"
        else "snowflake_staging_connection_3x"
    )

# For backwards compatibility, create a property-like access
class _LazyConstant:
    # Static constants that don't require DB access
    DAGS_FOLDER = "/usr/local/airflow/dags"
    SQL_FOLDER = os.path.join(DAGS_FOLDER, "swat_alerts/sql")
    SNOWFLAKE_SWAT_VIEWS = "ALERTS"
    SWAT_YAML_FOLDER = os.path.join(DAGS_FOLDER, "swat_alerts/yaml")
    S3_CONN_ID = None
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
    
    @property
    def SNOWFLAKE_CONN_ID(self):
        return get_snowflake_conn_id()
    
    @property
    def ENVIRONMENT_FLAG(self):
        return _get_environment_flag()
    
    @property
    def S3_DATALAKE_BUCKET(self):
        env: str = _get_environment_flag()
        return {
            "prod": "prod-datalake-internal",
            "staging": "stage-datalake-internal",
        }[env]
    
    @property
    def S3_DATAENGINEERING_BUCKET(self):
        env: str = _get_environment_flag()
        return {
            "prod": "above-snowflake",
            "staging": "above-snowflake",
        }[env]
    
    def _get_db_lookup(self):
        env: str = _get_environment_flag()
        return {
            "staging": {
                "RAW_DATABASE_NAME": "ABOVE_DW_STG",
                "CURATED_DATABASE_NAME": "CURATED_STG",
                "TRUSTED_DATABASE_NAME": "TRUSTED_DEV",
                "STORAGE_INTEGRATION_NAME": "ABOVELENDING_DATALAKE_STAGE",
                "UTILS_DATABASE_NAME": "UTILS_STG",
            },
            "prod": {
                "RAW_DATABASE_NAME": "ABOVE_DW_PROD",
                "REFINED_DATABASE_NAME": "REFINED_PROD",
                "CURATED_DATABASE_NAME": "CURATED_PROD",
                "TRUSTED_DATABASE_NAME": "TRUSTED",
                "STORAGE_INTEGRATION_NAME": "ABOVELENDING_DATALAKE_PROD",
                "UTILS_DATABASE_NAME": "UTILS_PROD",
            },
        }[env]
    
    @property
    def RAW_DATABASE_NAME(self):
        return self._get_db_lookup()["RAW_DATABASE_NAME"]
    
    @property
    def CURATED_DATABASE_NAME(self):
        return self._get_db_lookup()["CURATED_DATABASE_NAME"]
    
    @property
    def TRUSTED_DATABASE_NAME(self):
        return self._get_db_lookup()["TRUSTED_DATABASE_NAME"]
    
    @property
    def STORAGE_INTEGRATION_NAME(self):
        return self._get_db_lookup()["STORAGE_INTEGRATION_NAME"]
    
    @property
    def UTILS_DATABASE_NAME(self):
        return self._get_db_lookup()["UTILS_DATABASE_NAME"]

# Create a single lazy instance that other modules can import and use
# Usage: from above.common.constants import lazy_constants
# Then: lazy_constants.SNOWFLAKE_CONN_ID, lazy_constants.ENVIRONMENT_FLAG, etc.
lazy_constants = _LazyConstant()

# For backwards compatibility, keep these as callable functions
def get_environment_flag():
    return _get_environment_flag()

def get_s3_datalake_bucket():
    env = _get_environment_flag()
    return {
        "prod": "prod-datalake-internal",
        "staging": "stage-datalake-internal",
    }[env]

def get_s3_dataengineering_bucket():
    env = _get_environment_flag()
    return {
        "prod": "above-snowflake",
        "staging": "above-snowflake",
    }[env]
