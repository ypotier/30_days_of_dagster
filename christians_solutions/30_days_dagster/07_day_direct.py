# prompt: Implement an asset downstream of your first asset. Explore ways of passing data between assets.
import dagster as dg
import pandas as pd
from dagster_pandas.data_frame import create_table_schema_metadata_from_dataframe
from typing import Tuple
import os


parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_dir = os.path.join(parent_dir, 'data')

@dg.asset(
        automation_condition=dg.AutomationCondition.on_cron("* * * * *"),
        retry_policy=dg.RetryPolicy(max_retries=2),
        # add definition metadata
        description="Data about orders for a fictional company",
        owners=["christian@dagsterlabs.com"],
        tags={"category": "ingestion", "priority": "high"},
        kinds=["file","csv", "pandas"],
)
def orders(context: dg.AssetExecutionContext) -> None:
    
    orders = pd.read_csv(os.path.join(data_dir, 'orders_raw.csv'))
    # add metadata to the structrued event log
    context.log.info(f"Creating asset one with data: {orders.head()}")
    orders.to_csv(os.path.join(data_dir,"orders.csv") )
    return dg.MaterializeResult(# add runtime metadata
        metadata={
            "dagster/row_count": dg.MetadataValue.int(len(orders)), 
            "preview": dg.MetadataValue.md(orders.head().to_markdown()),
            "dagster/column_schema": create_table_schema_metadata_from_dataframe(orders)
        }
    )


@dg.asset(
        deps=["orders"],
        automation_condition=dg.AutomationCondition.any_downstream_conditions(),
        kinds={"file","csv", "pandas"},
)
def orders_summary(context: dg.AssetExecutionContext) -> None:
    orders = pd.read_csv(os.path.join(data_dir, 'orders.csv'))
    # pandas to aggregate orders df by size
    orders_summary = orders.groupby("size")["price"].agg([
        ("total_orders", "count"),
        ("total_revenue", "sum")
    ]).reset_index()
    context.log.info(f"Creating asset two {orders.head()}")
    context.log.info(f"Orders size: {orders_summary}")
    orders_summary.to_csv(os.path.join(data_dir,"orders_summary.csv") )
    return dg.MaterializeResult(# add runtime metadata
         metadata={
            "dagster/row_count": dg.MetadataValue.int(len(orders_summary)), 
            "preview": dg.MetadataValue.md(orders_summary.head().to_markdown()),
            "dagster/column_schema": create_table_schema_metadata_from_dataframe(orders_summary)
        }
    )

@dg.asset(
        deps=["orders_summary"],
         automation_condition=dg.AutomationCondition.on_cron("*/10 * * * *")
)
def asset_three(context: dg.AssetExecutionContext) -> None:
    context.log.info("Creating asset three")

defs = dg.Definitions(
    assets= [orders, orders_summary, asset_three],)