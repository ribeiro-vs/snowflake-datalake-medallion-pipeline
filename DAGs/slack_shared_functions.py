from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from jinja2 import Template
from logging_config import logger
from airflow.models import Variable

SLACK_BOT_TOKEN = Variable.get("slack_secret_token")
SLACK_CHANNEL = Variable.get("alert_channel")
FAILURE_SLACK_MESSAGE = """
*ATTENTION!* <!channel> The dag *{{ ti.dag_id }}* has *just* failed executing! :exclamation:

>*Task:* `{{ ti.task_id }}`
>*Execution date:* `{{ ds }}`
>*State:* `{{ ti.state }}`

More information about the error in <http://localhost:9090/dags/{{ ti.dag_id }}/grid|*here*>.
"""

def slack_notifier(context):
    """
    Sends a notification to a specified Slack channel when a DAG fails.
    
    Parameters:
    context (dict): Context dictionary provided by Airflow containing task instance (ti) 
                    and other useful information such as execution date (ds).

    The function:
    - Extracts task instance and execution details from the context.
    - Uses a Jinja2 template to format a failure message.
    - Sends the formatted message to a Slack channel via the Slack API.
    - Logs success or failure of the Slack notification.
    """
    
    # Extract task instance (ti) and other context variables
    ti = context.get('ti')
    ds = context.get('ds')

    # Render Slack message
    message = Template(FAILURE_SLACK_MESSAGE).render(ti=ti, ds=ds)

    # Initialize Slack client using the bot token
    client = WebClient(token=SLACK_BOT_TOKEN)

    try:
        # Send the message to the specified channel using the Slack bot
        response = client.chat_postMessage(
            channel=SLACK_CHANNEL,
            text=message,
            mrkdwn=True
        )
        logger.info(f"Slack notification sent successfully: {response['ts']}")
    except SlackApiError as e:
        # Handle errors when sending the message
        logger.error(f"Failed to send Slack notification: {e.response['error']}")