import os

from airflow.configuration import conf
from airflow.models import Variable

# Use environment variable instead of Airflow Variable to avoid DB call during import
# Set ENVIRONMENT_FLAG in your Airflow environment (e.g., in docker-compose or MWAA environment)
ENVIRONMENT_FLAG = os.getenv("ENVIRONMENT_FLAG", "staging")

# DAGS_FOLDER = conf["core"]["dags_folder"] # KeyError with 2.10.1
DAGS_FOLDER = "/usr/local/airflow/dags"
SQL_FOLDER = os.path.join(DAGS_FOLDER, "swat_alerts/sql")

SNOWFLAKE_SWAT_VIEWS = "ALERTS"
SWAT_YAML_FOLDER = os.path.join(DAGS_FOLDER, "swat_alerts/yaml")


S3_CONN_ID = None
SNOWFLAKE_CONN_ID = (
    "snowflake_prod_connection"
    if ENVIRONMENT_FLAG == "prod"
    else "snowflake_staging_connection_3x"
)
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
S3_DATALAKE_BUCKET = {
    "prod": "prod-datalake-internal",
    "staging": "stage-datalake-internal",
    "local": "stage-datalake-internal",  # Use staging bucket for local dev
}[ENVIRONMENT_FLAG]
S3_DATAENGINEERING_BUCKET = {
    "prod": "above-snowflake",
    "staging": "above-snowflake",
    "local": "above-snowflake",  # Use staging bucket for local dev
}[ENVIRONMENT_FLAG]

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
    "local": {  # Use staging config for local development
        "RAW_DATABASE_NAME": "ABOVE_DW_STG",
        "CURATED_DATABASE_NAME": "CURATED_STG",
        "TRUSTED_DATABASE_NAME": "TRUSTED_DEV",
        "STORAGE_INTEGRATION_NAME": "ABOVELENDING_DATALAKE_STAGE",
        "UTILS_DATABASE_NAME": "UTILS_STG",
    },
}[ENVIRONMENT_FLAG]

RAW_DATABASE_NAME = _db_lookup["RAW_DATABASE_NAME"]
CURATED_DATABASE_NAME = _db_lookup["CURATED_DATABASE_NAME"]
TRUSTED_DATABASE_NAME = _db_lookup["TRUSTED_DATABASE_NAME"]
STORAGE_INTEGRATION_NAME = _db_lookup["STORAGE_INTEGRATION_NAME"]
UTILS_DATABASE_NAME = _db_lookup["UTILS_DATABASE_NAME"]
