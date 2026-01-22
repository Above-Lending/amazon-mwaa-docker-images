"""Shared utilities for Twilio operations."""

import json
import logging
import os
from typing import Any, Dict

from airflow.exceptions import AirflowException
from airflow.sdk import Variable
from jinja2 import Template
from twilio.rest import Client

from above.common.constants import lazy_constants

logger = logging.getLogger(__name__)
# Path to SQL files
SQL_DIR: str = os.path.join(os.path.dirname(os.path.dirname(__file__)), "sql")

# Module-level default constants (avoid referencing lazy_constants at module level for conditional logic)
def get_lookup_limit() -> int:
    """Get the lookup limit based on environment."""
    return 5000 if lazy_constants.ENVIRONMENT_FLAG == "prod" else 5

def get_twilio_client() -> Client:
    """
    Initialize and return Twilio client from Airflow Variables.

    Returns:
        Configured Twilio client

    Raises:
        AirflowException: If Twilio credentials are missing or invalid
    """
    try:
        twilio_credentials: Dict = json.loads(Variable.get("twilio"))
        account_sid: str | None = twilio_credentials.get("TWILIO_ACCOUNT_SID")
        auth_token: str | None = twilio_credentials.get("TWILIO_AUTH_TOKEN")

        if not account_sid or not auth_token:
            raise AirflowException("Twilio credentials incomplete")

        return Client(account_sid, auth_token)
    except Exception as e:
        logger.error(f"Failed to initialize Twilio client: {e}")
        raise AirflowException(f"Failed to initialize Twilio client: {e}")


def load_sql_file(filename: str, sql_dir: str | None = None) -> str:
    """
    Load SQL file from the sql directory.

    Args:
        filename: Name of the SQL file to load
        sql_dir: Optional custom SQL directory path. If None, uses default SQL_DIR

    Returns:
        SQL content as string

    Raises:
        AirflowException: If file not found or error loading file
    """
    directory = sql_dir or SQL_DIR
    sql_path: str = os.path.join(directory, filename)
    try:
        with open(sql_path, "r") as f:
            return f.read()
    except FileNotFoundError:
        raise AirflowException(f"SQL file not found: {sql_path}")
    except Exception as e:
        raise AirflowException(f"Error loading SQL file {sql_path}: {e}")


def render_sql_template(template_content: str, params: Dict[str, Any]) -> str:
    """
    Render SQL template with Jinja2.

    Args:
        template_content: SQL template content
        params: Parameters to render in the template

    Returns:
        Rendered SQL string
    """
    template = Template(template_content)
    return template.render(params=params)


def build_active_numbers_query(
    trusted_database: str,
    raw_database: str,
    raw_schema: str,
    raw_table: str,
    lookup_refresh_months: int,
) -> str:
    """
    Build SQL query to find active phone numbers needing lookup.

    Args:
        trusted_database: Name of trusted database
        raw_database: Name of raw database
        raw_schema: Name of raw schema
        raw_table: Name of raw table
        lookup_refresh_months: Number of months before lookup refresh needed

    Returns:
        SQL query string
    """
    template_content: str = load_sql_file("active_numbers_needing_lookup.sql")
    return render_sql_template(
        template_content,
        {
            "trusted_database": trusted_database,
            "raw_database": raw_database,
            "raw_schema": raw_schema,
            "raw_table": raw_table,
            "lookup_refresh_months": lookup_refresh_months,
        },
    )


def build_merge_query(
    raw_database: str,
    raw_schema: str,
    raw_table: str,
    suffix: str,
    merge_columns: list[str],
) -> str:
    """
    Build the SQL MERGE statement for updating the reverse lookups table.

    Args:
        raw_database: Name of raw database
        raw_schema: Name of raw schema
        raw_table: Name of raw table
        suffix: Suffix for the updates table name
        merge_columns: List of all columns for the merge operation

    Returns:
        SQL MERGE statement
    """
    # Columns to update (all except the primary key)
    update_columns = [col for col in merge_columns if col != "PHONE_NUMBER_E164"]

    template_content: str = load_sql_file("merge_reverse_lookups.sql")
    rendered = render_sql_template(
        template_content,
        {
            "raw_database": raw_database,
            "raw_schema": raw_schema,
            "raw_table": raw_table,
            "suffix": suffix,
            "update_columns": update_columns,
            "insert_columns": merge_columns,
        },
    )
    logger.info("Rendered template: %s", rendered)
    return rendered
