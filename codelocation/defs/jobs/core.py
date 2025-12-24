import dagster as dg

# Daily non-paid job
all_asset_job = dg.define_asset_job(
    name="hourly_all",
)
daily_schedule = dg.ScheduleDefinition(
    name="hourly",
    cron_schedule="30 * * * *",  # Runs every hour at minute 30
    target=all_asset_job,
)