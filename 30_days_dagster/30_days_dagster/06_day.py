# Instead of running asset A every minute, figure out a way to make asset A only run when there is a 
# change to your CSV file.

#prompt  Update the code from asset A to read CSV from disk. 
import dagster as dg
import random
import pandas as pd
import os

csv_external_asset = dg.AssetSpec("csv_external_asset")


@dg.sensor(minimum_interval_seconds=30)
def csv_external_asset_sensor(
    context: dg.SensorEvaluationContext,
) -> dg.SensorResult:
    # Poll the external system every 30 seconds
    # for the last time the file was modified
    file_last_modified_at_ms = os.path.getmtime("data/orders.csv") * 1000

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
        automation_condition=dg.AutomationCondition.eager(),
        retry_policy=dg.RetryPolicy(max_retries=2),
        deps=[csv_external_asset]
)
def asset_one(context: dg.AssetExecutionContext) -> None:
    random_choice = random.random()
    context.log.info(f"Random choice: {random_choice}")
    if random_choice < 0.5:
        raise dg.Failure(
            description=f"Asset one failed because the choice was {random_choice}",
            metadata={
                "filepath": dg.MetadataValue.path("some_path/to_file"),
                "dashboard_url": dg.MetadataValue.url("http://mycoolsite.com/failures"),
            },
        )
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(parent_dir, 'data')
    df = pd.read_csv(os.path.join(data_dir, 'orders.csv'))
    # add metadata to the structrued event log
    context.log.info(f"Creating asset one with data: {df.head()}")
    return dg.MaterializeResult(
        metadata={
            "dagster/row_count": dg.MetadataValue.int(len(df)), 
            "preview": dg.MetadataValue.md(df.head().to_markdown()),
            "filepath": dg.MetadataValue.path(os.path.join(data_dir, 'orders.csv')),
        }
    )

@dg.asset(
        deps=["asset_one"],
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
    assets= [asset_one, asset_two, asset_three],
    sensors=[csv_external_asset_sensor],
)