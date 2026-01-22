"""Twilio Reverse Number Lookups DAG.

This DAG performs reverse lookups on phone numbers using the Twilio API
and loads the results into Snowflake. Uses TwilioLookupOperator for reusable logic.
"""

import os

from airflow.decorators import dag
from pendulum import datetime, duration

from above.common.slack_alert import task_failure_slack_alert
from above.common.constants import lazy_constants
from twilio_communications.operators.twilio_lookup_operator import TwilioLookupOperator

# Constants
DAG_ID = os.path.basename(__file__).replace(".py", "")
DAG_START_DATE = datetime(2024, 6, 1, tz="UTC")

@dag(
    dag_id=DAG_ID,
    description="Refreshes reverse lookups and loads into the data warehouse",
    tags=["twilio", "non_alert"],
    schedule="55 11 * * *",  # Daily 0555 winter/0655 summer CT
    start_date=DAG_START_DATE,
    max_active_runs=1,
    catchup=False,
    default_args={
        "owner": "Data Engineering",
        "depends_on_past": False,
        "retries": 0,  # Manually retry only after manual dbt re-rerun
        "retry_delay": duration(minutes=10),
        "execution_timeout": duration(minutes=60),
        "on_failure_callback": (
            task_failure_slack_alert if lazy_constants.ENVIRONMENT_FLAG == "prod" else None
        ),
    },
)
def twilio_reverse_number_lookups():
    """Twilio Reverse Number Lookups DAG."""

    TwilioLookupOperator(task_id="fetch_and_lookup_numbers")

twilio_reverse_number_lookups()
