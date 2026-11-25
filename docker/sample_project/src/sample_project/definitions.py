from dagster import Definitions, with_source_code_references

from sample_project.assets import (
    cleaned_orders,
    daily_revenue,
    flaky_enrichment,
    raw_orders,
)
from sample_project.jobs import flaky_debug_job, orders_pipeline_job
from sample_project.schedules import daily_orders_schedule
from sample_project.sensors import new_orders_sensor

defs = Definitions(
    assets=with_source_code_references(
        [raw_orders, cleaned_orders, daily_revenue, flaky_enrichment]
    ),
    jobs=[orders_pipeline_job, flaky_debug_job],
    schedules=[daily_orders_schedule],
    sensors=[new_orders_sensor],
)
