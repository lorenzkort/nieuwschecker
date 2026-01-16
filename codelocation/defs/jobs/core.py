import dagster as dg

# Daily non-paid job
all_asset_job = dg.define_asset_job(
    name="daily_all",
)
daily_schedule = dg.ScheduleDefinition(
    name="daily",
    cron_schedule="0 */3 * * *",  # Runs every 3rd hour at minute 0
    target=all_asset_job,
    default_status= dg.DefaultScheduleStatus.RUNNING
)