import logging
import os

from pendulum import datetime, DateTime

from airflow.decorators import dag
from tableau.operators.tableau_operator import TableauOperator

from above.common.constants import (
    S3_CONN_ID,
    S3_DATALAKE_BUCKET,
    TABLEAU_SNAPSHOT_BUCKET_DIRECTORY,
    TABLEAU_SERVER_URL,
    TABLEAU_SITE_ID,
)
from tableau.utils.tableau_utils import get_tableau_dag_default_args

logger: logging.Logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

this_filename: str = str(os.path.basename(__file__).replace(".py", ""))
dag_start_date: DateTime = datetime(2024, 6, 1, tz="UTC")


@dag(
    dag_id=this_filename,
    description="Snapshots all Tableau Files.",
    tags=["data", "tableau", "snapshot"],
    schedule="20 22 * * SUN",  # 17:20 CST (22:20 UTC) every Sunday
    start_date=dag_start_date,
    max_active_runs=1,
    catchup=False,
    default_args=get_tableau_dag_default_args(),
)
def tableau_snapshot() -> None:
    """
    DAG to snapshot all Tableau workbooks weekly.

    Takes a complete snapshot of all workbooks updated in the last 10 years
    for archival and recovery purposes.
    """
    snapshot_task: TableauOperator = TableauOperator(
        task_id="snapshot_tableau",
        updated_since=r"{{ data_interval_start.subtract(years=10).strftime('%Y-%m-%dT%H:%M:%SZ') }}",
        site_id=TABLEAU_SITE_ID,
        server_url=TABLEAU_SERVER_URL,
        s3_bucket=S3_DATALAKE_BUCKET,
        s3_directory=(
            TABLEAU_SNAPSHOT_BUCKET_DIRECTORY
            + "/{{data_interval_start.strftime('%Y-%m-%d')}}"
        ),
        s3_conn_id=S3_CONN_ID,
    )

    snapshot_task


tableau_snapshot()
