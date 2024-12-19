# prompt: One of the interesting corollaries to `multi_assets` discussed in day 16 is conditional branching. Can you implement an asset pipeline where:

# - Asset A returns a value greater than 10 about 30% of the time
# - If asset A >10, run asset B. If asset A <= 10, run asset C.

import dagster as dg
import numpy as np

@dg.multi_asset(
        retry_policy=dg.RetryPolicy(max_retries=2),
        specs=[
    # define asset-level metadata here
    dg.AssetSpec(
        "asset_a",
        description="The raw orders data",
        owners=["christian@dagsterlabs.com"],
        tags={"category": "ingestion", "priority": "high"},
        kinds=["file","csv"],
        automation_condition=dg.AutomationCondition.on_cron("* * * * *")
    ),
    dg.AssetSpec(
        "asset_b",
        description="Medium Sized Orders",
        owners=["somebody_else@dagsterlabs.com"],
        deps=["asset_a"],
        tags={"category": "transformation", "priority": "medium"},
        kinds=["pandas","dataframe"],
        automation_condition=dg.AutomationCondition.eager(),
        # set to skippable
        skippable=True),
    dg.AssetSpec("asset_c",
        description="Summarized Order Data",
        owners=["another_person@dagsterlabs.com"],
        deps=["asset_a"],
        tags={"category": "transformation", "priority": "medium"},
        kinds=["pandas","dataframe"],
        automation_condition=dg.AutomationCondition.eager(),
        # set to skippable
        skippable=True),
    ])
def orders_assets(context: dg.AssetExecutionContext):
    # asset_a returns a value of 10 30% of the time
    asset_a = 10 if np.random.rand() < 0.3 else 11
    # yield a MaterializeResult for each asset
    yield dg.MaterializeResult(
        asset_key="asset_a",
    )
    if asset_a > 10:
        yield dg.MaterializeResult(
            asset_key="asset_b",
        )
    
    if asset_a <= 10:
        yield dg.MaterializeResult(
            asset_key="asset_c",
        )


defs = dg.Definitions(
    assets= [orders_assets])