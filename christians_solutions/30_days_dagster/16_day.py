# prompt: The inverse of day 15. Sometimes one step creates more than one asset. How would you model 1 step to many assets?

import dagster as dg
import pandas as pd
from dagster_pandas.data_frame import create_table_schema_metadata_from_dataframe
import os


class CsvStorageResource(dg.ConfigurableResource):
    base_dir: str = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data",
    )

    def read_data(self, csv_file):
        return pd.read_csv(os.path.join(self.base_dir, csv_file) )
    
    def write_data(self, df, csv_file):
        return df.to_csv(os.path.join(self.base_dir, csv_file), index=False )
    
    def return_path(self):
        return self.base_dir

@dg.multi_asset(
        retry_policy=dg.RetryPolicy(max_retries=2),
        specs=[
    # define asset-level metadata here
    dg.AssetSpec(
        "orders",
        description="The raw orders data",
        owners=["christian@dagsterlabs.com"],
        tags={"category": "ingestion", "priority": "high"},
        kinds=["file","csv"],
        automation_condition=dg.AutomationCondition.on_cron("* * * * *")
    ),
    dg.AssetSpec(
        "orders_medium",
        description="Medium Sized Orders",
        owners=["somebody_else@dagsterlabs.com"],
        deps=["orders"],
        tags={"category": "transformation", "priority": "medium"},
        kinds=["pandas","dataframe"],
        automation_condition=dg.AutomationCondition.eager()),
    dg.AssetSpec("orders_summary",
        description="Summarized Order Data",
        owners=["another_person@dagsterlabs.com"],
        deps=["orders"],
        tags={"category": "transformation", "priority": "medium"},
        kinds=["pandas","dataframe"],
        automation_condition=dg.AutomationCondition.eager()),
    ])
def orders_assets(context: dg.AssetExecutionContext, csv_storage_resource: CsvStorageResource):
    # operations needed to create and log the materialization of the orders asset
    orders = csv_storage_resource.read_data('orders_raw.csv')
    csv_storage_resource.write_data(orders, 'orders.csv')
    # yield a MaterializeResult for each asset
    yield dg.MaterializeResult(
        asset_key="orders",
        metadata={
            "dagster/row_count": dg.MetadataValue.int(len(orders)), 
            "preview": dg.MetadataValue.md(orders.head().to_markdown()),
            "dagster/column_schema": create_table_schema_metadata_from_dataframe(orders)
        }
    )

    # operations needed to create and log the materialization of the orders_medium asset
    orders_medium = orders[orders["size"] == "Medium"]
    csv_storage_resource.write_data(orders_medium, 'orders_medium.csv')
    yield dg.MaterializeResult(
        asset_key="orders_medium",
        metadata={
            "dagster/row_count": dg.MetadataValue.int(len(orders_medium)), 
            "preview": dg.MetadataValue.md(orders_medium.head().to_markdown()),
            "dagster/column_schema": create_table_schema_metadata_from_dataframe(orders_medium)
        }
    )

    # operations needed to create and log the materialization of the orders_summary asset
    orders_summary = orders.groupby("size")["price"].agg([
        ("total_orders", "count"),
        ("total_revenue", "sum")
    ]).reset_index()
    csv_storage_resource.write_data(orders_summary, 'orders_summary.csv')
    yield dg.MaterializeResult(
        asset_key="orders_summary",
        metadata={
            "dagster/row_count": dg.MetadataValue.int(len(orders_summary)), 
            "preview": dg.MetadataValue.md(orders_summary.head().to_markdown()),
            "dagster/column_schema": create_table_schema_metadata_from_dataframe(orders_summary)
        }
    )

defs = dg.Definitions(
    assets= [orders_assets],
    resources={"csv_storage_resource": CsvStorageResource(base_dir="/Users/christian/code/30_days_of_dagster/data_prod")},)