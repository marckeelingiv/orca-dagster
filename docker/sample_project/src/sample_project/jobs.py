from dagster import AssetSelection, define_asset_job

orders_pipeline_job = define_asset_job(
    name="orders_pipeline_job",
    selection=AssetSelection.assets("raw_orders", "cleaned_orders"),
    description="Materializes raw_orders and cleaned_orders.",
)

flaky_debug_job = define_asset_job(
    name="flaky_debug_job",
    selection=AssetSelection.assets("flaky_enrichment"),
    description="Runs flaky_enrichment to exercise failure logs and agent handoff.",
)
