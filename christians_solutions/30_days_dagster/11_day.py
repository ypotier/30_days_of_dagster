# Now that the data pipeline has two meaningful assets, we want to prevent errors in data quality.
# Add an asset check to ensure asset A does not pass bad data to asset B.

import dagster as dg
import pandas as pd
import os
from dagster_pandas.data_frame import create_table_schema_metadata_from_dataframe

# parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# data_dir = os.path.join(parent_dir, 'data')


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


class myPathConfig(dg.Config):
    file_path: str

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
def orders(context: dg.AssetExecutionContext, csv_storage: CsvStorageResource) -> None:
    orders = csv_storage.read_data('orders_raw.csv')
    context.log.info(f"Creating asset one with data: {orders.head()}")
    # directly write csv to storage
    csv_storage.write_data(orders, 'orders.csv')
    return dg.MaterializeResult(
        metadata={
            "dagster/row_count": dg.MetadataValue.int(len(orders)), 
            "preview": dg.MetadataValue.md(orders.head().to_markdown()),
            "dagster/column_schema": create_table_schema_metadata_from_dataframe(orders)
        }
    )


@dg.asset_check(asset="orders")
def orders_increasing_or_equal(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    histotical_metadata = context.instance.fetch_materializations(
        dg.AssetRecordsFilter(
            asset_key=dg.AssetKey("orders"),
         ),
         limit=2
    )

    if len(histotical_metadata) < 2:
        return dg.AssetCheckResult(passed=True)
    row_count_values = [
        record.asset_event.metadata.get("dagster/row_count") for record in histotical_metadata[0]
    ]
    latest_row_count = row_count_values[0]
    previous_row_count = row_count_values[1]
    
    return dg.AssetCheckResult(
        passed=latest_row_count>=previous_row_count,
        metadata={"latest_row_count": latest_row_count, "previous_row_count": previous_row_count}
    )


# sensor to poll the external csv for changes and register a materialization event it changes
@dg.sensor(minimum_interval_seconds=30)
def csv_external_asset_sensor(
    context: dg.SensorEvaluationContext,
    csv_storage: CsvStorageResource,
) -> dg.SensorResult:
    # Poll the external system every 30 seconds
    # for the last time the file was modified
    file_last_modified_at_ms = os.path.getmtime(os.path.join(csv_storage.return_path, "orders_raw.csv") ) * 1000

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
def orders_summary(
    context: dg.AssetExecutionContext,
    csv_storage: CsvStorageResource,
                   ) -> None:
    # directly read csv from storage
    orders = csv_storage.read_data('orders.csv')
    orders_summary = orders.groupby("size")["price"].agg([
        ("total_orders", "count"),
        ("total_revenue", "sum")
    ]).reset_index()
    context.log.info(f"Creating asset two {orders.head()}")
    # directly write csv to storage
    csv_storage.write_data(orders_summary, 'orders_summary.csv')

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

# define the resources to use different folders for different "environments"
resource_defs = {
        "DEV":{"csv_storage": CsvStorageResource(base_dir="/Users/christian/code/30_days_of_dagster/data_dev")},
        "PROD": {"csv_storage": CsvStorageResource(base_dir="/Users/christian/code/30_days_of_dagster/data_prod")}
    }

def get_env():
    # defined in .env file, which dagster dev automatically loads
    if os.getenv("DAGSTER_PROD_DEPLOY", "") == "1":
        return "PROD"
    elif os.getenv("DAGSTER_IS_DEV_CLI"):
        return "DEV"
    else:
        return "UNDEFINED"

defs = dg.Definitions(
    assets= [orders, orders_summary, asset_three, csv_external_asset],
    asset_checks=[orders_increasing_or_equal],
    sensors=[csv_external_asset_sensor],
    # get the resource for the environment defined
    resources=resource_defs[get_env()]
)