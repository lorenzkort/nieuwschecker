import dagster as dg

# All Asset Job
all_asset_job = dg.define_asset_job(
    name="all_asset_job",
)
daily_schedule = dg.ScheduleDefinition(
    name="all_asset_schedule",
    cron_schedule="0 */3 * * *",  # Runs every 3rd hour at minute 0
    target=all_asset_job,
    default_status= dg.DefaultScheduleStatus.RUNNING
)