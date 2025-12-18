from airflow.models import TaskInstance
from airflow.sdk.types import RuntimeTaskInstanceProtocol
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from airflow.sdk import Context


def task_failure_slack_alert(context: Context) -> None:
    """
    Creates a task failure alert using SlackWebhookOperator.

    We should use this method inside of tasks.
    """
    task_instance: RuntimeTaskInstanceProtocol | None = context.get("task_instance")
    timestamp: str | None = context.get("ts")

    if not task_instance:
        raise ValueError("task_instance is required in context")
    if not timestamp:
        raise ValueError("timestamp is required in context")

    slack_message_data: dict[str, str] = {
        "dag": task_instance.dag_id,
        "task": task_instance.task_id,
        "timestamp": timestamp
    }
    slack_message_template: str = """
            :red_circle: Task Failed
            *Dag*: {dag}
            *Task*: {task}
            *Execution Time*: {timestamp}
            """

    slack_message: str = slack_message_template.format(**slack_message_data)

    failed_alert: SlackWebhookOperator = SlackWebhookOperator(
        task_id="slack_notification",
        slack_webhook_conn_id="slack_webhook",
        message=slack_message,
    )

    failed_alert.execute(context)


def task_failure_slack_alert_hook(context: Context) -> None:
    """
    Creates a task failure alert using SlackWebhookHook.

    We should use this method outside of tasks, like in callbacks.
    """
    task_instance: RuntimeTaskInstanceProtocol | None = context.get("task_instance")
    timestamp: str | None = context.get("ts")

    if not task_instance:
        raise ValueError("task_instance is required in context")
    if not timestamp:
        raise ValueError("timestamp is required in context")
    
    slack_message_data: dict[str, str] = {
        "dag": task_instance.dag_id,
        "task": task_instance.task_id,
        "timestamp": timestamp
    }

    slack_message_template: str = """
            :red_circle: Task Failed
            *Dag*: {dag}
            *Task*: {task}
            *Execution Time*: {timestamp}
            """

    slack_message: str = slack_message_template.format(**slack_message_data)

    slack_hook: SlackWebhookHook = SlackWebhookHook(
        slack_webhook_conn_id="slack_webhook",
    )

    slack_hook.send(text=slack_message)
