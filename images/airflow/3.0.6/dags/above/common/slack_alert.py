from airflow.sdk.types import RuntimeTaskInstanceProtocol
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

from airflow.sdk.definitions.context import Context


def task_failure_slack_alert(channel: str = "C08E2H7TYAD"):
    """
    Returns a callback function that sends failure alerts to the specified Slack channel.

    Args:
        channel: Slack channel to send alerts to.  Use SLACK CHANNEL ID, not name.  Default is "C08E2H7TYAD", "debug-swat".

    Usage:
        task = PythonOperator(
            task_id='example_task',
            python_callable=example_function,
            on_failure_callback=task_failure_slack_alert(channel='ABCDE12345),
        )
    """

    def callback(context: Context, timeout: int = 15) -> None:
        task_instance: RuntimeTaskInstanceProtocol | None = context.get("task_instance")
        timestamp: str | None = context.get("ts")
        exception: str = str(
            context.get("exception", "No exception information available.")
        )

        if not task_instance:
            raise ValueError("task_instance is required in context")
        if not timestamp:
            raise ValueError("timestamp is required in context")

        slack_message_data: dict[str, str] = {
            "dag": task_instance.dag_id,
            "task": task_instance.task_id,
            "timestamp": timestamp,
            "exception": exception,
        }

        slack_message_template: str = """
            :red_circle: Task Failed
            *Dag*: {dag}
            *Task*: {task}
            *Execution Time*: {timestamp}
            *Exception*: {exception}
            """

        slack_message: str = slack_message_template.format(**slack_message_data)

        SlackAPIPostOperator(
            task_id="slack_notification",
            slack_conn_id="slack_api_3x",
            channel=channel,
            text=slack_message,
            timeout=timeout,
        ).execute(context)

    return callback


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
        "timestamp": timestamp,
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
