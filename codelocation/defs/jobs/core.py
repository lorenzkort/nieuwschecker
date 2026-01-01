import dagster as dg

# Daily non-paid job
all_asset_job = dg.define_asset_job(
    name="daily_all",
)
daily_schedule = dg.ScheduleDefinition(
    name="daily",
    cron_schedule="0 22 * * *",  # Runs every day at 23:00
    target=all_asset_job,
    default_status= dg.DefaultScheduleStatus.RUNNING
)