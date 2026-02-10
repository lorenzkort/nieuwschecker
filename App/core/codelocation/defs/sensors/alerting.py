import os

import dagster as dg
from dagster_slack import make_slack_on_run_failure_sensor


def detailed_failure_message(context: dg.RunFailureSensorContext) -> str:
    """Create a detailed failure message with error information"""

    run = context.dagster_run
    failure_event = context.failure_event

    # Build the failure message
    message_parts = [
        "🚨 *Job Failed*",
        f"*Job:* `{run.job_name}`",
        f"*Run ID:* `{run.run_id}`",
        f"*Status:* {run.status}",
    ]

    # Add error message if available
    if failure_event and failure_event.message:
        error_msg = failure_event.message[:500]  # Truncate to 500 chars
        message_parts.append(f"*Error:* ```{error_msg}```")

    # Get step failure details if available
    step_failure_events = context.get_step_failure_events()
    if step_failure_events:
        error_messages = []
        for event in step_failure_events:
            if event.event_specific_data and hasattr(
                event.event_specific_data, "error"
            ):
                error_msg = event.event_specific_data.error.message[:200]  # Truncate
                stack = event.event_specific_data.error.stack[:500]  # Truncate
                error_messages.append(f"Step {event.step_key}: {error_msg}")
        if error_messages:
            message_parts.append(
                "*Step Failures:*\n" + "\n".join(error_messages[:2])
            )  # Max 2 steps

    # Ensure total message is under 2800 chars (leaving buffer)
    full_message = "\n".join(message_parts)
    if len(full_message) > 2800:
        full_message = full_message[:2800] + "... (truncated)"

    return full_message


# Create the sensor
slack_on_run_failure = make_slack_on_run_failure_sensor(
    channel="#alerts",  # Your Slack channel
    slack_token=os.getenv("SLACK_BOT_TOKEN") or "",  # Your Slack bot token
    text_fn=detailed_failure_message,
    name="slack_failure_alerts",  # Custom sensor name
    webserver_base_url="http://127.0.0.1:3000/",  # Your Dagster instance URL
    minimum_interval_seconds=60,  # Minimum time between sensor evaluations
)
