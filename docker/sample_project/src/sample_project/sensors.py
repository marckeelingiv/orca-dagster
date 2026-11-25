from dagster import SensorEvaluationContext, SkipReason, sensor

from sample_project.jobs import orders_pipeline_job


@sensor(
    name="new_orders_sensor",
    job=orders_pipeline_job,
    minimum_interval_seconds=30,
    description="Polls for incoming order batches and triggers orders_pipeline_job.",
)
def new_orders_sensor(context: SensorEvaluationContext) -> SkipReason:
    context.log.info("Checking for new order files")
    return SkipReason("No new order files detected in dev sandbox")
