# prompt: Update the code from yesterday to include meaningful definition time and runtime metadata about asset A. 
# Also log something useful to the event log.
import dagster as dg
import pandas as pd
from dagster_pandas.data_frame import create_table_schema_metadata_from_dataframe
import os


@dg.asset(
        automation_condition=dg.AutomationCondition.on_cron("* * * * *"),
        retry_policy=dg.RetryPolicy(max_retries=2),
        # add definition metadata
        description="Data about orders for a fictional company",
        owners=["christian@dagsterlabs.com"],
        tags={"category": "ingestion", "priority": "high"},
        kinds=["file","csv"],
)
def orders(context: dg.AssetExecutionContext) -> None:
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(parent_dir, 'data')
    df = pd.read_csv(os.path.join(data_dir, 'orders.csv'))

    # add metadata to the structrued event log
    context.log.info(f"Creating asset one with data: {df.head()}")
    return dg.MaterializeResult(
        # add runtime metadata
        metadata={
            "dagster/row_count": dg.MetadataValue.int(len(df)), 
            "preview": dg.MetadataValue.md(df.head().to_markdown()),
            "filepath": dg.MetadataValue.path(os.path.join(data_dir, 'orders.csv')),
            "dagster/column_schema": create_table_schema_metadata_from_dataframe(df)
        }
    )

@dg.asset(
        deps=["orders"],
        automation_condition=dg.AutomationCondition.any_downstream_conditions()
)
def asset_two(context: dg.AssetExecutionContext) -> None:
    context.log.info("Creating asset two")

@dg.asset(
        deps=["asset_two"],
         automation_condition=dg.AutomationCondition.on_cron("*/10 * * * *")
)
def asset_three(context: dg.AssetExecutionContext) -> None:
    context.log.info("Creating asset three")

defs = dg.Definitions(
    assets= [orders, asset_two, asset_three],
)