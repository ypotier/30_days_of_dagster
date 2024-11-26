import dagster as dg

@dg.asset
def asset_one(context: dg.AssetExecutionContext) -> None:
    context.log.info("Creating asset one")

@dg.asset(
        deps=["asset_one"]
)
def asset_two(context: dg.AssetExecutionContext) -> None:
    context.log.info("Creating asset two")


@dg.asset(
        deps=["asset_two"]
)
def asset_three(context: dg.AssetExecutionContext) -> None:
    context.log.info("Creating asset three")

defs = dg.Definitions(
    assets= [asset_one, asset_two, asset_three],
)
