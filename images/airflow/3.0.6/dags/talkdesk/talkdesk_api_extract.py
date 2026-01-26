from __future__ import annotations

import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from time import sleep
from typing import Any, Dict, List

import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import get_current_context
from airflow.timetables.interval import CronDataIntervalTimetable

from above.common.constants import lazy_constants
from above.common.slack_alert import task_failure_slack_alert
from talkdesk.common.talkdesk import get_talkdesk_api_auth_token
from above.common.utils import put_df_to_s3_bucket_name
from above.operators.preprocess_files_operator import (
    PreprocessFilesOperator,
    UnzipSplitFilesOperator
)

logger: logging.Logger = logging.getLogger(__name__)
this_filename: str = str(os.path.basename(__file__).replace(".py", ""))
logger.setLevel(logging.DEBUG)

DATA_SOURCE: str = "talkdesk_api"
TALKDESK_REPORTS: List[str] = ["calls", "user_status", "advanced_dialer_calls_report"]
DATESTAMP: str = date.today().isoformat()
S3_SOURCE_DIR: str = os.path.join(lazy_constants.DATALAKE_SOURCE_DIR, DATA_SOURCE)
S3_PREPROCESSED_DIR: str = os.path.join(lazy_constants.DATALAKE_PREPROCESSED_DIR, DATA_SOURCE)
S3_ERROR_DIR: str = os.path.join(lazy_constants.DATALAKE_ERROR_DIR, DATA_SOURCE, DATESTAMP)
S3_SUCCESS_DIR: str = os.path.join(lazy_constants.DATALAKE_SUCCESS_DIR, DATA_SOURCE, DATESTAMP)
S3_SOURCE_ARCHIVE_DIR: str = os.path.join(
    lazy_constants.DATALAKE_SOURCE_ARCHIVE_DIR,
    DATA_SOURCE,
    DATESTAMP
)

start_date: datetime = datetime(2024, 1, 13, tzinfo=timezone.utc)


@task.short_circuit
def check_data_interval_times():
    context = get_current_context()
    data_interval_start: datetime | None = context.get("data_interval_start")
    previous_end: datetime | None = context.get("prev_data_interval_end_success")
    logging.info(f"Resuming at: {data_interval_start} from {previous_end}")
    
    # If either is None, or if data_interval_start is newer than previous_end, proceed
    if data_interval_start is None or previous_end is None:
        return True
    
    is_new: bool = data_interval_start >= previous_end
    return is_new

def get_interval_datetimes() -> Dict[str, str]:
    context = get_current_context()
    data_interval_start: datetime | None = context.get("data_interval_start")
    data_interval_end: datetime | None = context.get("data_interval_end")
    
    if data_interval_start is None or data_interval_end is None:
        logging.error("data_interval_start or data_interval_end is None")
        return {}
    
    logging.info(f"data_interval_start: {data_interval_start}")
    logging.info(f"data_interval_end: {data_interval_end}")
    
    # If start and end are the same (can happen in Airflow 3.x), calculate a 1-hour interval
    if data_interval_start >= data_interval_end:
        logging.warning(f"Start and end are equal, calculating 1-hour interval from execution date")
        execution_date = context.get("logical_date") or data_interval_start
        data_interval_start = execution_date
        data_interval_end = execution_date + timedelta(hours=1)
        logging.info(f"Adjusted to: start={data_interval_start}, end={data_interval_end}")
    
    time_format: str = "%Y-%m-%dT%H:%M:%S.000Z"
    talkdesk_report_start: str = data_interval_start.strftime(time_format)
    talkdesk_report_end: str = data_interval_end.strftime(time_format)
    
    logging.info(f"Formatted start: {talkdesk_report_start}, end: {talkdesk_report_end}")
    
    return {
        "start": talkdesk_report_start,
        "end": talkdesk_report_end
    }


@task
def get_talkdesk_report_to_s3(report):
    # Retrieve auth token at execution time, not at DAG parse time
    talkdesk_auth_token = get_talkdesk_api_auth_token()
    
    # Get interval datetimes
    intervals = get_interval_datetimes()
    if not intervals:
        return False
    
    talkdesk_report_start = intervals["start"]
    talkdesk_report_end = intervals["end"]
    
    talkdesk_report_request_body: Dict = dict(
        format="json",
        timespan={"from": talkdesk_report_start, "to": talkdesk_report_end}
    )

    headers = {
        "Authorization": f"Bearer {talkdesk_auth_token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    base_url: str = "https://api.talkdeskapp.com/data/reports"
    jobs_url: str = f"{base_url}/{report}/jobs"
    
    logging.info(f"POST URL: {jobs_url}")
    logging.info(f"Request body: {json.dumps(talkdesk_report_request_body)}")
    
    response: requests.Response = requests.post(
        url=jobs_url,
        headers=headers,
        json=talkdesk_report_request_body
    )
    
    # 200 or 202 are both valid success responses
    if response.status_code not in [200, 202]:
        logging.error(f"API Error {response.status_code}: {response.text}")
        response.raise_for_status()
    else:
        logging.info(f"Job created successfully with status {response.status_code}")
    talkdesk_job: Dict[str, Any] = json.loads(response.content.decode())["job"]
    talkdesk_job_id: str = talkdesk_job.get("id")

    job_url: str = f"{jobs_url}/{talkdesk_job_id}"
    headers = {
        "Authorization": f"Bearer {talkdesk_auth_token}",
        "Accept": "application/json"
    }
    talkdesk_data_entries: List | None = None
    # It usually takes 1-2 seconds to generate the report
    sleep_interval_in_seconds: float = 3
    while not talkdesk_data_entries:
        sleep(sleep_interval_in_seconds)
        logging.info(f"Requesting job {talkdesk_job_id}...")
        response = requests.get(url=job_url, headers=headers)
        content: Dict[str, Any] = json.loads(response.content.decode())
        status: str | None = content.get("job", {}).get("status")
        if status == "failed":
            logging.error("Talkdesk job failed with API status '%s'!", status)
            return False
        elif status:
            logging.info("API status: %s", status)
        # An "entries" element will appear with the returned data
        if "entries" in content.keys():
            talkdesk_data_entries = content.get("entries", [])
            break

    if not len(talkdesk_data_entries):
        return False

    logging.info(
        f"Received {len(talkdesk_data_entries)} rows,"
        f" {sys.getsizeof(talkdesk_data_entries)} bytes"
    )
    entries_df: pd.DataFrame = pd.DataFrame(talkdesk_data_entries)
    logging.info("Data Frame size: {}".format(sys.getsizeof(entries_df)))
    logging.info("Data Frame: {}".format(entries_df))
    filename = (
        f"{report}_{talkdesk_report_start}_{talkdesk_report_end}.json.gz"
    )
    logging.info(f"Writing data frame to file {filename}...")
    put_df_to_s3_bucket_name(
        df=entries_df,
        s3_conn_id=lazy_constants.S3_CONN_ID,
        s3_bucket_name=lazy_constants.S3_DATALAKE_BUCKET,
        s3_key=os.path.join(S3_SOURCE_DIR, filename),
        file_format='json'
    )
    return True


with DAG(
        dag_id=this_filename,
        description=f"TalkDesk {', '.join(TALKDESK_REPORTS)}"
                    f" from API to Data Warehouse",
        tags=["talkdesk", "non_alert"],
        # every 15 minutes after the hour
        schedule = CronDataIntervalTimetable("15 * * * *", timezone="UTC"),
        start_date=start_date,
        max_active_runs=1,
        catchup=False,
        default_args=dict(
            owner="Data Engineering",
            start_date=start_date,
            depends_on_past=False,
            retries=3,
            retry_delay=timedelta(minutes=45),
            execution_timeout=timedelta(minutes=9),
            on_failure_callback=task_failure_slack_alert
        )
) as dag:

    unzip_split = UnzipSplitFilesOperator(
        task_id="unzip_split",
        s3_conn_id=lazy_constants.S3_CONN_ID,
        s3_in_bucket=lazy_constants.S3_DATALAKE_BUCKET,
        s3_source_dir=S3_SOURCE_DIR,
        s3_source_archive_dir=S3_SOURCE_ARCHIVE_DIR,
    )

    preprocess = PreprocessFilesOperator(
        task_id="preprocess",
        s3_conn_id=lazy_constants.S3_CONN_ID,
        s3_in_bucket=lazy_constants.S3_DATALAKE_BUCKET,
        s3_source_dir=S3_SOURCE_DIR,
        s3_out_bucket=lazy_constants.S3_DATALAKE_BUCKET,
        s3_preprocessed_dir=S3_PREPROCESSED_DIR,
        s3_error_dir=S3_ERROR_DIR,
        s3_success_dir=S3_SUCCESS_DIR,
        file_parsing_args={"sep": ","},
    )

    trigger_load_dag = TriggerDagRunOperator(
        task_id="trigger_dag_{}_load".format(DATA_SOURCE),
        trigger_dag_id="{}_load".format(DATA_SOURCE),
    )

    chain(
        # check_data_interval_times(),
        [get_talkdesk_report_to_s3.override(task_id=f"get_talkdesk_{report}")
         (report) for report in TALKDESK_REPORTS],
        unzip_split,
        preprocess,
        trigger_load_dag
    )
