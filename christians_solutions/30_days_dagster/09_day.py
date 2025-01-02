# Day 9 Update asset A so that the filepath it is responsible for loading can be passed as configuration. 

import dagster as dg
import pandas as pd
import os
from dagster_pandas.data_frame import create_table_schema_metadata_from_dataframe

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_dir = os.path.join(parent_dir, 'data')

class myPathConfig(dg.Config):
    file_path: str = data_dir

# define an asset that represents an external CSV file
csv_external_asset = dg.AssetSpec(
    key = "csv_external_asset",
    description="A CSV file that is external to Dagster. If you have issues call 866-5309",
    metadata={"file_path": dg.MetadataValue.path("data/orders_raw.csv"),
            },
    owners=["team:Upstream_Ops", "upstream_ops_person@company.com"],
    kinds=["file","csv"],
)

@dg.asset(
        deps=[csv_external_asset],
        automation_condition=dg.AutomationCondition.on_cron("* * * * *"),
)
def orders(context: dg.AssetExecutionContext, config: myPathConfig) -> None:
    orders = pd.read_csv(os.path.join(config.file_path, 'orders_raw.csv'))
    context.log.info(f"Creating asset one with data: {orders.head()}")
    # directly write csv to storage
    orders.to_csv(os.path.join(data_dir,"orders.csv") )
    return dg.MaterializeResult(
        metadata={
            "dagster/row_count": dg.MetadataValue.int(len(orders)), 
            "preview": dg.MetadataValue.md(orders.head().to_markdown()),
            "dagster/column_schema": create_table_schema_metadata_from_dataframe(orders)
        }
    )



# sensor to poll the external csv for changes and register a materialization event it changes
@dg.sensor(minimum_interval_seconds=30)
def csv_external_asset_sensor(
    context: dg.SensorEvaluationContext,
) -> dg.SensorResult:
    # Poll the external system every 30 seconds
    # for the last time the file was modified
    file_last_modified_at_ms = os.path.getmtime(os.path.join(data_dir, "orders_raw.csv") ) * 1000

    # Use the cursor to store the last time the sensor updated the asset
    if context.cursor is not None:
        external_asset_last_updated_at_ms = float(context.cursor)
    else:
        external_asset_last_updated_at_ms = 0

    if file_last_modified_at_ms > external_asset_last_updated_at_ms:
        # The external asset has been modified since it was last updated,
        # so record a materialization and update the cursor.
        return dg.SensorResult(
            asset_events=[
                dg.AssetMaterialization(
                    asset_key=csv_external_asset.key,
                    # You can optionally attach metadata
                    metadata={"file_last_modified_at_ms": file_last_modified_at_ms},
                )
            ],
            cursor=str(file_last_modified_at_ms),
        )
    else:
        # Nothing has happened since the last check
        return dg.SensorResult()




@dg.asset(
        deps=["orders"],
        automation_condition=dg.AutomationCondition.eager(),
)
def orders_summary(context: dg.AssetExecutionContext) -> None:
    # directly read csv from storage
    orders = pd.read_csv(os.path.join(data_dir, 'orders.csv'))
    orders_summary = orders.groupby("size")["price"].agg([
        ("total_orders", "count"),
        ("total_revenue", "sum")
    ]).reset_index()
    context.log.info(f"Creating asset two {orders.head()}")
    # directly write csv to storage
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
    assets= [orders, orders_summary, asset_three, csv_external_asset],
    sensors=[csv_external_asset_sensor],
)