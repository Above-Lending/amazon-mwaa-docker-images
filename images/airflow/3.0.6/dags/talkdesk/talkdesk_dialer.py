import json
import logging
import os
from datetime import date
from textwrap import dedent
from typing import Dict, List, Any

import pandas as pd
import requests
from airflow.exceptions import AirflowException
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pandas import DataFrame
from pendulum import datetime, now, duration
from sqlalchemy.engine import Engine
from typing_extensions import LiteralString
from above.common.constants import lazy_constants

from above.common.constants import SNOWFLAKE_CONN_ID
from above.common.slack_alert import task_failure_slack_alert
from talkdesk.common.talkdesk import get_talkdesk_api_auth_token
from above.common.constants import ENVIRONMENT_FLAG



logger: logging.Logger = logging.getLogger(__name__)
this_modules_name: str = str(os.path.basename(__file__).replace(".py", ""))
start_date: datetime = datetime(2024, 6, 1, tz="UTC")
RECORD_LISTS_URL = "https://api.talkdeskapp.com/record-lists"

def insert_log(table_name: str, values: Dict[str, Any], fail_on_error: bool = False ) -> None:
    if not values:
        raise ValueError("Values dictionary cannot be empty")

    columns = ", ".join(values.keys())
    placeholders = ", ".join(["%s"] * len(values))
    params = list(values.values())

    sql = f"""INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"""

    try:
        snowflake_hook = SnowflakeHook(SNOWFLAKE_CONN_ID)
        snowflake_hook.run(sql, parameters=params)

        logger.info("Inserted log into %s | values=%s", table_name, values)

    except Exception as exc:
        logger.exception("Failed to insert log into Snowflake | table=%s | values=%s", table_name, values)

        if fail_on_error:
            raise AirflowException(
                f"Snowflake log insert failed for table {table_name}"
            ) from exc



def query_to_dataframe(query: str) -> DataFrame:
    """
    Executes Snowflake query and returns result
    :param query:
    :return: result
    """
    logger.info(f"Executing query:\n{query}")
    snowflake_hook: SnowflakeHook = SnowflakeHook(lazy_constantsSNOWFLAKE_CONN_ID)
    sql_engine: Engine = snowflake_hook.get_sqlalchemy_engine()
    dataframe: DataFrame = pd.read_sql(query, sql_engine)
    return dataframe


@task.short_circuit
def check_is_business_day(**context):
    """
    Checks current date against the business calendar to run only during
    business days and not weekends or holidays.
    """
    date_today: str = date.today().strftime("%Y-%m-%d")
    query: str = dedent(
        f"""
        SELECT 
          CONTAINS(
            UTIL_DB.PUBLIC.BUSINESS_DAYS_IN_WEEK_ABOVE_HOLIDAYS('{date_today}')::varchar
            , '{date_today}') AS "IS_TODAY_A_BUSINESS_DAY"
        """
    )
    dataframe: DataFrame = query_to_dataframe(query)

    is_today_a_business_day: bool = (
        False if dataframe.empty else dataframe.is_today_a_business_day.values[0]
    )
    logger.info(f"IS_TODAY_A_BUSINESS_DAY: {is_today_a_business_day}")
    return is_today_a_business_day


@task
def check_dialer_list_freshness():
    """
    Checks most recent dbt load time of collections queues table and raises
    an exception if it isn't the current day's collections queue.
    """
    query: str = dedent(
        """
        SELECT TOP 1
            CONVERT_TIMEZONE('UTC', _DBT_LOAD_TIME_CST)::DATE AS "table_date"
        FROM
            CURATED_PROD.REPORTING.VW_COLLECTIONS_ELIGIBLE
        ORDER BY
            _DBT_LOAD_TIME_CST DESC
        """
    )
    dataframe: DataFrame = query_to_dataframe(query)
    if dataframe.empty:
        raise AirflowException("No collection queue table date found")

    table_date: date = dataframe.table_date.values[0]
    logger.info(f"Collections queue date is {table_date}")
    if table_date != date.today():
        raise AirflowException(f"Collections table is out of date")


def delete_record(auth_token: str, record_list_id: str, record_id: str):
    url = f"{RECORD_LISTS_URL}/{record_list_id}/records/{record_id}"

    headers = {
        "Authorization": f"Bearer {auth_token}"
    }

    try:
        logger.info(f"record_id = {record_id}")
        res = requests.delete(url, headers=headers, timeout=30)
        res.raise_for_status()

        logger.info( "Deleted Talkdesk record_id=%s record_list_id=%s", record_id, record_list_id,)

    except requests.RequestException as e:
        logger.error(
            "Failed to delete Talkdesk record "
            "record_id=%s record_list_id=%s error=%s response=%s",
            record_id,
            record_list_id,
            e,
            getattr(e.response, "text", None),
        )
        raise


def create_record_list(auth_token: str, record_list_name: str) -> str:
    # Creates a new Talkdesk Record List and returns the ID of
    body: Dict = dict(name=record_list_name)
    headers: Dict = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json",
        "Accept": "application/json, application/hal+json",
    }
    response: requests.Response = requests.post(url=RECORD_LISTS_URL, headers=headers, json=body)
    response.raise_for_status()
    response_json: Dict = response.json()
    record_list_id: str = response_json.get("id")
    logger.info(
        f"Create Record List response:"
        f" {response.status_code}<{response.reason}>"
        f" • Name: \"{response_json.get('name')}\""
        f" • Status: {response_json.get('status')}"
        f" • ID: {record_list_id}"
    )
    return record_list_id



@task
def create_talkdesk_record_lists(auth_token: str, view_name: str, record_list_name_suffix: str):
    if  lazy_constants.ENVIRONMENT_FLAG == "prod":
        timestamp = now("America/Chicago").strftime("%Y-%m-%d %H:%M%Z %A")
        record_list_name = f"Airflow {timestamp} • {record_list_name_suffix}"
        record_list_id = create_record_list(auth_token=auth_token, record_list_name=record_list_name)
        add_records_to_list(auth_token, view_name, record_list_id)    


@task
def add_records(auth_token: str, view_name: str, record_list_id: str):
    if  ENVIRONMENT_FLAG == "prod":
        add_records_to_list(auth_token, view_name, record_list_id)


def add_records_to_list(auth_token: str, view_name: str, record_list_id: str):
    dataframe: DataFrame = query_to_dataframe(f"Select * from {view_name}" )
    result: List[Dict] = [json.loads(value) for value in dataframe.record_json.values]
    offset, limit = 0, 25
    total = len(result)
    total_inserted = 0
    while offset < total:
        chunk = result[offset:offset + limit]
        response = requests.post(
            url=f"{RECORD_LISTS_URL}/{record_list_id}/records/bulks",
            headers={
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json",
                "Accept": "application/json, application/hal+json",
            },
            json={"records": chunk},
        )
        response.raise_for_status()
        body = response.json()
        created = body.get("total_created", 0)
        errors = body.get("total_errors", 0)
        total_inserted += created

        if created == len(chunk) and not errors:
            logger.info(
                f"Success: {created} records sent • Offset {offset} • ID {record_list_id}"
            )
        else:
            logger.warning(
                f"Issues: {created}/{len(chunk)} created • {errors} errors • Offset {offset} • ID {record_list_id}"
            )

        offset += limit
    insert_log(table_name="AIRFLOW.TALKDESK.INSERTED_RECORDS",
        values={
            "RECORD_LIST_ID": record_list_id,
            "TOTAL_RECORD_NUM": total,
            "INSERTED_RECORD_NUM": total_inserted
        }
    )

@task 
def purge_list(auth_token: str, record_list_id: str, limit: int = 100):
    if lazy_constants.ENVIRONMENT_FLAG == "prod":
        url = f"{RECORD_LISTS_URL}/{record_list_id}/records"
        offset = 0
        total_deleted = 0
        total_records = 0

        while True:
            params = {"limit": limit, "offset": offset}

            try:
                res = requests.get(url, headers={"Authorization": f"Bearer {auth_token}"}, params=params, timeout=30)
                res.raise_for_status()
            except requests.RequestException as e:
                logger.error(
                    "Failed to fetch Talkdesk records record_list_id=%s offset=%s error=%s response=%s",
                    record_list_id, offset, e, getattr(e.response, "text", None)
                )
                raise

            try:
                records = res.json().get("_embedded", {}).get("records", [])
            except ValueError:
                logger.error("Invalid JSON response from Talkdesk record_list_id=%s offset=%s response=%s",
                            record_list_id, offset, res.text)
                raise

            if not records:
                logger.info("No more records to fetch for record_list_id=%s", record_list_id)
                break
            total_records += len(records)
            for record in records:
                record_id = record.get("id")
                if record_id:
                    try:
                        delete_record(auth_token, record_list_id, record_id)
                        total_deleted += 1
                    except Exception:
                        logger.warning(
                            "Failed to delete record_id=%s record_list_id=%s, skipping",
                            record_id, record_list_id
                        )

            offset += limit

        logger.info("Completed deletion for record_list_id=%s total_deleted=%s", record_list_id, total_deleted)
        insert_log(table_name="AIRFLOW.TALKDESK.PURGED_RECORDS",
            values={
                "RECORD_LIST_ID": record_list_id,
                "TOTAL_RECORD_NUM": total_records,
                "PURGED_RECORD_NUM": total_deleted
            }
        )
        return total_deleted



@dag(
    dag_id=this_modules_name,
    description="Uploads dialer list from the Data Warehouse to TalkDesk",
    tags=["data", "talkdesk"],
    schedule="12 10 * * 0-5",  # Sun-Fri 0412 CST/0512 CDT
    start_date=start_date,
    max_active_runs=1,
    catchup=False,
    default_args=dict(
        owner="Data Engineering",
        start_date=start_date,
        depends_on_past=False,
        retries=0,
        retry_delay=duration(minutes=3),
        execution_timeout=duration(minutes=10),
        on_failure_callback=task_failure_slack_alert,
    ),
)

def talkdesk_dialer():
    auth_token: str = get_talkdesk_api_auth_token()
    LIST_ID="d9be9636-528f-45e8-bdf8-27ffd9ee6085"
    chain(
        check_is_business_day(),
        check_dialer_list_freshness(),
        create_talkdesk_record_lists(auth_token,'airflow.talkdesk.dpd_one_list', '1_119_dpd'),
        purge_list(auth_token, LIST_ID),
        add_records(auth_token, "airflow.talkdesk.DPD_RANDOM_ROTATION", LIST_ID)
    )


talkdesk_dialer()
