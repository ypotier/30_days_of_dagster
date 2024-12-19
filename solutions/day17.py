from dagster import multi_asset, AssetSpec, MaterializeResult
import random

# https://youtu.be/MgkniMnmhMU

@multi_asset(
        specs=[AssetSpec('a', skippable=True), AssetSpec('b', skippable=True)]
)
def my_expensive_operation():

    if random.randint(1,2) > 1:
        yield MaterializeResult(asset_key="a")
    else:
        yield MaterializeResult(asset_key="b")