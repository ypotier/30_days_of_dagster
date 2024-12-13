#prompt  Update the code from asset A to read CSV from disk. 
import dagster as dg
import random
import pandas as pd
from dagster_pandas.data_frame import create_table_schema_metadata_from_dataframe
import os


@dg.asset(
        automation_condition=dg.AutomationCondition.on_cron("* * * * *"),
        retry_policy=dg.RetryPolicy(max_retries=2)
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
            "dagster/column_schema": create_table_schema_metadata_from_dataframe(df)
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
)