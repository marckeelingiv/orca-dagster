from dagster import ScheduleDefinition

from sample_project.jobs import orders_pipeline_job

daily_orders_schedule = ScheduleDefinition(
    name="daily_orders_schedule",
    job=orders_pipeline_job,
    cron_schedule="0 6 * * *",
    description="Runs the commerce orders pipeline daily at 06:00 UTC.",
)
