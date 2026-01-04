import dagster as dg

# Daily non-paid job
all_asset_job = dg.define_asset_job(
    name="daily_all",
)
daily_schedule = dg.ScheduleDefinition(
    name="daily",
    cron_schedule="0 10,22 * * *",  # Runs every day at 10:00 and 23:00 (1 hour delay)
    target=all_asset_job,
    default_status= dg.DefaultScheduleStatus.RUNNING
)