from dagster import (
    AssetExecutionContext,
    Config,
    DailyPartitionsDefinition,
    MaterializeResult,
    AssetKey,
    TableColumn,
    TableColumnDep,
    TableColumnLineage,
    TableSchema,
    asset,
)

daily_partitions = DailyPartitionsDefinition(start_date="2026-11-01")


class FlakyConfig(Config):
    should_fail: bool = True


@asset(group_name="commerce", description="Ingests raw order records from the source feed.")
def raw_orders(context: AssetExecutionContext) -> list[dict[str, object]]:
    orders = [
        {"id": 1001, "currency": "USD", "amount": 49.50},
        {"id": 1002, "currency": "USD", "amount": 120.00},
        {"id": 1003, "currency": "EUR", "amount": 85.25},
    ]
    context.log.info(f"Loaded {len(orders)} raw orders")
    context.add_output_metadata({"dagster/column_schema": TableSchema(columns=[TableColumn("id", "int"), TableColumn("amount", "float")])})
    return orders


@asset(group_name="commerce", description="Normalizes and validates raw order records.")
def cleaned_orders(
    context: AssetExecutionContext, raw_orders: list[dict[str, object]]
) -> list[dict[str, object]]:
    cleaned = [o for o in raw_orders if float(o["amount"]) > 0]
    context.log.info(f"Validated {len(cleaned)} cleaned orders")
    context.add_output_metadata({
        "dagster/column_schema": TableSchema(columns=[TableColumn("id", "int"), TableColumn("amount", "float")]),
        "dagster/column_lineage": TableColumnLineage(deps_by_column={
            "amount": [TableColumnDep(asset_key=AssetKey("raw_orders"), column_name="amount")]
        }),
    })
    return cleaned


@asset(
    group_name="commerce",
    partitions_def=daily_partitions,
    description="Aggregates daily revenue for a single partition date.",
)
def daily_revenue(
    context: AssetExecutionContext, cleaned_orders: list[dict[str, object]]
) -> MaterializeResult:
    partition_date = context.partition_key
    total = round(sum(float(o["amount"]) for o in cleaned_orders), 2)
    context.log.info(f"Computed daily revenue for {partition_date}: {total}")
    return MaterializeResult(
        metadata={
            "partition_date": partition_date,
            "total_revenue": total,
            "dagster/column_schema": TableSchema(columns=[TableColumn("total_revenue", "float")]),
            "dagster/column_lineage": TableColumnLineage(deps_by_column={
                "total_revenue": [TableColumnDep(asset_key=AssetKey("cleaned_orders"), column_name="amount")]
            }),
        }
    )


@asset(
    group_name="debug",
    description="Intentionally fails by default (set should_fail: false in run config to pass).",
)
def flaky_enrichment(context: AssetExecutionContext, config: FlakyConfig) -> MaterializeResult:
    context.log.info(f"Running flaky_enrichment with should_fail={config.should_fail}")
    if config.should_fail:
        raise RuntimeError(
            "Simulated enrichment failure: missing currency exchange rate for EUR"
        )
    return MaterializeResult(metadata={"enriched_records": 3, "status": "ok"})
