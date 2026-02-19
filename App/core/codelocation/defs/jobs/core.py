import dagster as dg

# All Asset Job
all_asset_job = dg.define_asset_job(
    name="all_asset_job",
)
daily_schedule = dg.ScheduleDefinition(
    name="all_asset_schedule",
    cron_schedule="5 */1 * * *",  # Runs every hour at minute 5
    target=all_asset_job,
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
