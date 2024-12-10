# The code for reading and writing files to disk is pretty repetitive. Encapsulate this code in a resource.

import dagster as dg
import pandas as pd
import os
from dagster_pandas.data_frame import create_table_schema_metadata_from_dataframe

# define a resource that can read and write data
class CsvStorageResource(dg.ConfigurableResource):
    base_dir: str = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data",
    )

    def read_data(self, csv_file):
        return pd.read_csv(os.path.join(self.base_dir, csv_file) )
    
    def write_data(self, df, csv_file):
        return df.to_csv(os.path.join(self.base_dir, csv_file), index=False )


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
    assets= [orders, csv_external_asset],
    # get the resource for the environment defined
    resources=resource_defs[get_env()]
)


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


