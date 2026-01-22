"""Twilio Lookup Operator for reverse phone number lookups."""

import logging
import time
from typing import Any, Optional, Dict, List

from pandas import DataFrame
from pendulum import DateTime, now, instance
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from twilio.base.exceptions import TwilioRestException
from twilio.rest import Client
from twilio.rest.lookups.v2.phone_number import PhoneNumberInstance

from above.common.constants import lazy_constants
from above.common.snowflake_utils import dataframe_to_snowflake, query_to_dataframe
from twilio_communications.common.twilio_utils import (
    get_twilio_client,
    build_active_numbers_query,
    build_merge_query,
    get_lookup_limit,
    RAW_SCHEMA_NAME,
    RAW_TABLE_NAME,
    MAX_FAILED_NUMBERS_TO_LOG,
    MERGE_COLUMNS,
)

logger = logging.getLogger(__name__)

LOOKUP_REFRESH_MONTHS: int = 12  # Refresh lookups older than this.
TWILIO_API_DELAY_SECONDS: float = 0.05  # Small delay between API calls
TWILIO_FIELDS = ["caller_name", "line_type_intelligence"]
RAW_SCHEMA_NAME: str = "TWILIO"

class TwilioLookupOperator(BaseOperator):
    """Operator for performing Twilio reverse phone number lookups and loading to Snowflake.
    This operator:
    - Queries Snowflake for phone numbers needing lookup/refresh
    - Performs Twilio API lookups with rate limiting
    - Processes responses and extracts relevant fields
    - Writes results to Snowflake using MERGE pattern
    Args:
        raw_table_name: Target Snowflake table name for lookup results.
        raw_schema_name: Snowflake schema name.
        twilio_fields: List of Twilio API fields to request.
        lookup_limit: Maximum number of lookups to perform (safety limit).
        lookup_refresh_months: Refresh lookups older than this many months.
        api_delay_seconds: Delay between API calls to avoid rate limits.
        max_failed_numbers_to_log: Maximum number of failed phone numbers to log.
        skip_on_empty: Skip execution if no numbers need lookup.
        write_to_snowflake: Whether to write results (False for testing).
        **kwargs: Additional BaseOperator arguments.
    """

    template_fields = ("lookup_limit", "lookup_refresh_months")

    def __init__(
        self,
        raw_table_name: str = RAW_TABLE_NAME,
        raw_schema_name: str = RAW_SCHEMA_NAME,
        twilio_fields: list[str] | None = TWILIO_FIELDS,
        lookup_limit: int = get_lookup_limit(),
        lookup_refresh_months: int = LOOKUP_REFRESH_MONTHS,
        api_delay_seconds: float = TWILIO_API_DELAY_SECONDS,
        max_failed_numbers_to_log: int = MAX_FAILED_NUMBERS_TO_LOG,
        skip_on_empty: bool = True,
        write_to_snowflake: bool = True,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.raw_table_name = raw_table_name
        self.raw_schema_name = raw_schema_name
        self.twilio_fields = twilio_fields or TWILIO_FIELDS
        self.lookup_limit = lookup_limit
        self.lookup_refresh_months = lookup_refresh_months
        self.api_delay_seconds = api_delay_seconds
        self.max_failed_numbers_to_log = max_failed_numbers_to_log
        self.skip_on_empty = skip_on_empty
        self.write_to_snowflake = write_to_snowflake

    def _build_active_numbers_query(self) -> str:
        """
        Build SQL query to find active phone numbers needing lookup.

        :return: SQL query string
        """
        return build_active_numbers_query(
            trusted_database=lazy_constants.TRUSTED_DATABASE_NAME,
            raw_database=lazy_constants.RAW_DATABASE_NAME,
            raw_schema=self.raw_schema_name,
            raw_table=self.raw_table_name,
            lookup_refresh_months=self.lookup_refresh_months,
        )

    def _build_merge_query(self, suffix: str) -> str:
        """
        Build the SQL MERGE statement for updating the reverse lookups table.

        :param suffix: Suffix for the updates table name
        :return: SQL MERGE statement
        """
        return build_merge_query(
            raw_database=lazy_constants.RAW_DATABASE_NAME,
            raw_schema=self.raw_schema_name,
            raw_table=self.raw_table_name,
            suffix=suffix,
            merge_columns=MERGE_COLUMNS,
        )

    def _get_twilio_client(self) -> Client:
        """
        Initialize and return Twilio client from Airflow Variables.

        :return: Configured Twilio client
        :raises AirflowException: If Twilio credentials are missing or invalid
        """
        return get_twilio_client()

    def _get_snowflake_hook(self) -> SnowflakeHook:
        """
        Initialize and return Snowflake hook configured for RAW database.

        :return: Configured SnowflakeHook
        """
        hook: SnowflakeHook = SnowflakeHook(lazy_constants.SNOWFLAKE_CONN_ID)
        hook.database = lazy_constants.RAW_DATABASE_NAME
        hook.schema = RAW_SCHEMA_NAME
        return hook

    def fetch_numbers_to_look_up(self) -> DataFrame:
        """
        Queries new leads/applicants' phone numbers and existing phone numbers
        that need a lookup refresh

        :return: DataFrame containing phone numbers needing reverse lookup
        :raises ValueError: If number of lookups exceeds safety limit
        """
        query: str = self._build_active_numbers_query()

        try:
            phone_numbers_df: DataFrame = query_to_dataframe(query)
        except Exception as e:
            logger.error(f"Failed to fetch phone numbers: {e}")
            raise AirflowException(f"Failed to fetch phone numbers from Snowflake: {e}")

        number_of_lookups: int = len(phone_numbers_df.index)

        if number_of_lookups > self.lookup_limit:
            warn_msg = (
                f"Number of phone numbers ({number_of_lookups}) exceeds "
                f"failsafe lookup limit {self.lookup_limit}."
                f"Limiting to lookup limit {self.lookup_limit}."
            )
            # TODO: Have a slack hook that pops this into the airflow chat as a warning.
            logger.error(warn_msg)

            # Set the number of phone numbers as the lookup limit.
            phone_numbers_df = phone_numbers_df.head(self.lookup_limit)

        logger.info(f"{number_of_lookups} phone numbers need reverse lookups")
        return phone_numbers_df

    def perform_twilio_lookup(
        self, client: Client, phone_number: str, last_lookup: Optional[DateTime]
    ) -> Optional[PhoneNumberInstance]:
        """
        Perform a single Twilio reverse lookup for a phone number.

        :param client: Twilio client instance
        :param phone_number: Phone number in E164 format
        :param last_lookup: Timestamp of last lookup, if any
        :return: PhoneNumberInstance or None if lookup fails
        """
        try:
            if last_lookup:
                last_verified_date: str = instance(last_lookup).format("YYYYMMDD")
            else:
                last_verified_date: str = "19700101"

            response: PhoneNumberInstance = client.lookups.v2.phone_numbers(
                phone_number
            ).fetch(
                fields=",".join(TWILIO_FIELDS), last_verified_date=last_verified_date
            )
            return response
        except TwilioRestException as e:
            logger.error(f"Twilio API error for {phone_number}: {e.code} - {e.msg}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error looking up {phone_number}: {e}")
            return None

    def process_lookup_response(
        self, phone_number: str, response: Optional[PhoneNumberInstance]
    ) -> Dict[str, Any]:
        """
        Process Twilio API response and extract relevant fields.

        :param phone_number: Phone number that was looked up
        :param response: Twilio API response
        :return: Dictionary of extracted fields
        """
        # Initialize result with default values
        result: Dict[str, Any] = {
            "phone_number_e164": phone_number,
            "_last_lookup": now("UTC"),
            "phone_number_national_format": None,
            "calling_country_code": None,
            "country_code": None,
            "is_valid": None,
            "validation_errors": None,
            "caller_name": None,
            "caller_type": None,
            "_error_code_caller": None,
            "phone_type": None,
            "carrier_name": None,
            "mobile_country_code": None,
            "mobile_network_code": None,
            "_error_code_line_type": None,
        }

        if not response:
            return result

        # Extract basic phone number info - simplified getattr usage
        for field_name, attr_name in [
            ("phone_number_national_format", "national_format"),
            ("calling_country_code", "calling_country_code"),
            ("country_code", "country_code"),
            ("is_valid", "valid"),
        ]:
            result[field_name] = getattr(response, attr_name, None)

        # Validation errors
        validation_errors: Optional[List] = getattr(response, "validation_errors", None)
        if validation_errors:
            logger.warning(f"Validation errors for {phone_number}: {validation_errors}")
            result["validation_errors"] = validation_errors

        # Caller name details
        caller_details = getattr(response, "caller_name", None)
        if caller_details:
            result["caller_name"] = caller_details.get("caller_name")
            result["caller_type"] = caller_details.get("caller_type")
            error_code = caller_details.get("error_code")
            if error_code:
                logger.warning(f"Caller error code for {phone_number}: {error_code}")
            result["_error_code_caller"] = error_code

        # Line type intelligence
        line_details = getattr(response, "line_type_intelligence", None)
        if line_details:
            result["phone_type"] = line_details.get("type")
            result["carrier_name"] = line_details.get("carrier_name")
            result["mobile_country_code"] = line_details.get("mobile_country_code")
            result["mobile_network_code"] = line_details.get("mobile_network_code")
            error_code = line_details.get("error_code")
            if error_code:
                logger.warning(f"Line type error code for {phone_number}: {error_code}")
            result["_error_code_line_type"] = error_code

        return result

    def fetch_and_lookup_numbers(self) -> None:
        """
        Fetches a dataframe of phone numbers with reverse lookup fields, populates
        the lookup fields via the Twilio API, writes them to a new update table,
        then merges the update table with the reverse lookup table.

        :return: None
        :raises AirflowException: If critical errors occur during processing
        """
        start_time: float = time.time()

        # Initialize clients and connections
        twilio_client: Client = self._get_twilio_client()
        snowflake_hook: SnowflakeHook = self._get_snowflake_hook()

        # Fetch numbers needing lookup
        df: DataFrame = self.fetch_numbers_to_look_up()
        if df.empty:
            logger.info("All phone numbers already have current lookup information")
            return

        logger.info(f"Starting reverse lookups for {len(df)} phone numbers")

        # Process lookups using batch approach
        lookup_results: List[Dict[str, Any]] = []
        success_count: int = 0
        failure_count: int = 0
        failed_numbers: List[str] = []

        for index in df.index:
            phone_number: str = str(df.at[index, "phone_number_e164"])
            last_lookup = df.at[index, "_last_lookup"]

            # Perform lookup
            response: Optional[PhoneNumberInstance] = self.perform_twilio_lookup(
                twilio_client, phone_number, last_lookup
            )

            # Process response
            result: Dict[str, Any] = self.process_lookup_response(
                phone_number, response
            )
            lookup_results.append(result)

            if response:
                success_count += 1
            else:
                failure_count += 1
                failed_numbers.append(phone_number)

            # Small delay to avoid hitting rate limits
            if index < len(df) - 1:  # Don't delay after last item
                time.sleep(TWILIO_API_DELAY_SECONDS)

        # Log results
        elapsed_time: float = time.time() - start_time
        logger.info(
            f"Completed {success_count} successful lookups, {failure_count} failures "
            f"in {elapsed_time:.2f} seconds"
        )

        if failed_numbers:
            num_to_log = min(len(failed_numbers), MAX_FAILED_NUMBERS_TO_LOG)
            logger.warning(
                f"Failed lookups for {len(failed_numbers)} numbers. "
                f"First {num_to_log}: {failed_numbers[:num_to_log]}"
            )

        # Convert results to DataFrame
        results_df: DataFrame = DataFrame(lookup_results)
        results_df["_airfloaded_at"] = now("UTC")
        results_df.columns = results_df.columns.str.upper()

        # Check if we have any successful results to write
        if success_count == 0:
            logger.warning(
                "All lookups failed! No data will be written to Snowflake. "
                "Please investigate the failures."
            )
            raise AirflowException(
                "All Twilio lookups failed - aborting to prevent data loss"
            )

        # Write to Snowflake with proper resource management
        suffix: str = "_UPDATES"
        update_table_name: str = f"{RAW_TABLE_NAME}{suffix}"

        if lazy_constants.ENVIRONMENT_FLAG == "prod":
            try:
                with snowflake_hook.get_conn() as snowflake_connection:
                    # Write updates table
                    dataframe_to_snowflake(
                        results_df,
                        database_name=lazy_constants.RAW_DATABASE_NAME,
                        schema_name=RAW_SCHEMA_NAME,
                        table_name=update_table_name,
                        overwrite=True,
                        snowflake_connection=snowflake_connection,
                    )

                    # Merge updates into main table
                    merge_query: str = self._build_merge_query(suffix)
                    logger.info(f"Executing merge query: {merge_query}")
                    cursor = snowflake_connection.cursor()
                    try:
                        cursor.execute(merge_query)
                        logger.info(
                            f"Successfully merged {len(results_df)} records into {RAW_TABLE_NAME}"
                        )
                    finally:
                        cursor.close()

            except Exception as e:
                logger.error(f"Failed to write to Snowflake: {e}")
                raise AirflowException(f"Snowflake operation failed: {e}")

        else:  # If staging...
            logger.info(
                "Skipping Snowflake write/merge since ENVIRONMENT_FLAG is not 'prod'.  Displaying top results:"
            )
            logger.info(results_df.head())

    def execute(self, context: dict[str, Any]) -> None:
        """Execute the Twilio lookup operator."""
        if self.skip_on_empty:
            phone_numbers_df: DataFrame = self.fetch_numbers_to_look_up()
            if phone_numbers_df.empty:
                logger.info(
                    "No phone numbers need reverse lookups. Skipping execution."
                )
                return

        if self.write_to_snowflake:
            self.fetch_and_lookup_numbers()

        else:
            logger.info("write_to_snowflake is False. Skipping data write.")
