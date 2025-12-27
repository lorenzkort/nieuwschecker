import dagster as dg

# Daily non-paid job
all_asset_job = dg.define_asset_job(
    name="daily_all",
)
daily_schedule = dg.ScheduleDefinition(
    name="daily",
    cron_schedule="0 4 * * *",  # Runs every hour at minute 30
    target=all_asset_job,
    default_status= dg.DefaultScheduleStatus.RUNNING
)