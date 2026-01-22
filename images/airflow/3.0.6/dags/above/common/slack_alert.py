from typing import Any
import json
import logging

from airflow.models import TaskInstance, Variable
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.slack.hooks.slack import WebClient
from airflow.utils.context import Context

from above.common.constants import lazy_constants


CHANNELS = {
    "airflow-notifications":         "C04DYE1KXK9",
    "airflow-notifications-staging": "C04EPA3K69J",
}
DEFAULT_TIMEOUT = 10

logger: logging.Logger = logging.getLogger(__name__)

# Slack settings
def task_failure_slack_alert(context: Context, target_channel: str=None) -> None:
    """
        Sends a Slack alert when a task fails using Webclient. 
     
        Defaults to airflow-notifications channel in production.
    """

    channel: str
    if lazy_constants.ENVIRONMENT_FLAG == "prod":
        channel = target_channel if target_channel else CHANNELS["airflow-notifications"]
    else:
        channel = CHANNELS["airflow-notifications-staging"]

    task_instance: TaskInstance = context.get("task_instance")
    timestamp: str = context.get("ts")

    slack_message_data: dict[str, str] = dict(
        dag=task_instance.dag_id,
        task=task_instance.task_id,
        timestamp=timestamp,
        log_url=task_instance.log_url,
    )

    slack_message_template: str = """
            :red_circle: Task Failed
            *Dag*: {dag}
            *Task*: {task}
            *Execution Time*: {timestamp}
            *Log*: <{log_url}|click here>
            """

    slack_message: str = slack_message_template.format(**slack_message_data)
    
    SLACK_TOKEN = json.loads(Variable.get("SLACK_TOKEN"))["token"] 

    slack_client: WebClient = WebClient(
        token=SLACK_TOKEN,
        timeout=DEFAULT_TIMEOUT,
    )
    slack_client.chat_postMessage(channel=channel, text=slack_message)


def task_failure_slack_alert_hook(context: Context) -> None:

    task_instance: TaskInstance = context.get("task_instance")
    timestamp: str = context.get("ts")

    slack_message_data: dict[str, str] = dict(
        dag=task_instance.dag_id,
        task=task_instance.task_id,
        timestamp=timestamp,
        log_url=task_instance.log_url,
    )

    slack_message_template: str = """
            :red_circle: Task Failed
            *Dag*: {dag}
            *Task*: {task}
            *Execution Time*: {timestamp}
            *Log*: <{log_url}|click here>
            """

    slack_message: str = slack_message_template.format(**slack_message_data)

    slack_hook: SlackWebhookHook = SlackWebhookHook(
        slack_webhook_conn_id="slack_webhook",
    )

    slack_hook.send(text=slack_message)