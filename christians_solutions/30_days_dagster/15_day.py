# prompt: Sometimes it takes more than one step to create a data asset. How would you model a many steps to one asset relationship in Dagster?

import dagster as dg
import pandas as pd
import os
from dagster_pandas.data_frame import create_table_schema_metadata_from_dataframe


parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_dir = os.path.join(parent_dir, 'data')

@dg.op
def ingest_orders_csv(context: dg.OpExecutionContext) -> pd.DataFrame:
    orders = pd.read_csv(os.path.join(data_dir, 'orders_raw.csv'))
    context.log.info(f"orders data ingested: {orders.head()}")
    orders.to_csv(os.path.join(data_dir,"orders.csv") )
    return orders

@dg.op
def transform_orders(context: dg.OpExecutionContext, orders: pd.DataFrame) -> pd.DataFrame:
    # filter to only the orders with a size of medium
    orders_transformed = orders[orders["size"] == "Medium"]
    context.log.info(f"Creating transform_orders: {orders_transformed.head()}")
    return orders_transformed

@dg.op
def unrelated_op(context: dg.OpExecutionContext):
    context.log.info("This is an unrelated op")


@dg.op
def orders_summary(context: dg.OpExecutionContext, orders: pd.DataFrame) -> pd.DataFrame:
    orders_summary = orders.groupby("size")["price"].agg([
        ("total_orders", "count"),
        ("total_revenue", "sum")
    ]).reset_index()
    context.log.info(f"Orders data summarized {orders_summary.head()}")
    context.add_output_metadata({
            "dagster/row_count": dg.MetadataValue.int(len(orders_summary)), 
            "preview": dg.MetadataValue.md(orders.head().to_markdown()),
            "filepath": dg.MetadataValue.path(os.path.join(data_dir, 'orders.csv')),
            "dagster/column_schema": create_table_schema_metadata_from_dataframe(orders)
    })
    return orders_summary
    

@dg.graph_asset(
        automation_condition=dg.AutomationCondition.on_cron("* * * * *"))
def orders() -> pd.DataFrame:
    return orders_summary(transform_orders(ingest_orders_csv()))



@dg.asset(
        deps=["orders"]
)
def asset_two(context: dg.AssetExecutionContext) -> None:
    context.log.info("Creating asset two")

@dg.asset(
        deps=["asset_two"]
)
def asset_three(context: dg.AssetExecutionContext) -> None:
    context.log.info("Creating asset three")

defs = dg.Definitions(
    assets= [orders, asset_two, asset_three],
)
