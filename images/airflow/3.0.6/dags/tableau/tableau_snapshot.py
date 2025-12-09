import logging
import os
import json

from pendulum import datetime, duration, DateTime

from airflow.decorators import dag
from airflow.models import Variable
from tableau.operators.tableau_operator import TableauOperator

from above.common.constants import (
    TABLEAU_SNAPSHOT_BUCKET_DIRECTORY,
    TABLEAU_SERVER_URL,
    TABLEAU_SITE_ID,
    S3_CONN_ID,
    get_s3_datalake_bucket,
)
from above.common.slack_alert import task_failure_slack_alert

logger: logging.Logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

this_filename: str = str(os.path.basename(__file__).replace(".py", ""))
dag_start_date: DateTime = datetime(2024, 6, 1, tz="UTC")


def _get_tableau_credentials() -> dict[str, str]:
    """Get Tableau credentials from Airflow Variables (lazy loaded)."""
    return json.loads(Variable.get("tableau"))


@dag(
    dag_id=this_filename,
    description="Snapshots all Tableau Files.",
    tags=["data", "tableau"],
    schedule="20 22 * * SUN",  # 17:20 CST (22:20 UTC) every Sunday
    start_date=dag_start_date,
    max_active_runs=1,
    catchup=False,
    default_args=dict(
        owner="Data Engineering",
        start_date=dag_start_date,
        depends_on_past=False,
        retries=0,  # Manually retry only after manual re-rerun
        retry_delay=duration(minutes=10),
        execution_timeout=duration(minutes=120),
        on_failure_callback=task_failure_slack_alert,
    ),
)
def run() -> None:
    """DAG to snapshot all Tableau workbooks to S3."""
    tableau_env = _get_tableau_credentials()
    s3_datalake_bucket = get_s3_datalake_bucket()
    
    tableau_snapshot: TableauOperator = TableauOperator(
        task_id="snapshot_tableau",
        updated_since=r"{{ data_interval_start.subtract(years=10).strftime('%Y-%m-%dT%H:%M:%SZ') }}",
        site_id=TABLEAU_SITE_ID,
        server_url=TABLEAU_SERVER_URL,
        token_name=tableau_env["TOKEN_NAME"],
        personal_access_token=tableau_env["TOKEN_SECRET"],
        s3_bucket=s3_datalake_bucket,
        s3_directory=(
            TABLEAU_SNAPSHOT_BUCKET_DIRECTORY
            + "/{{data_interval_start.strftime('%Y-%m-%d')}}"
        ),
        s3_conn_id=S3_CONN_ID,
    )

    tableau_snapshot


run()
