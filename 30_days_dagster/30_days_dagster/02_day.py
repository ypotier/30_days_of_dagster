#prompt Run the assets A, B, and C every minute on a schedule.
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

schedule_every_minute = dg.ScheduleDefinition(
    name="schedule_every_minute",
    target= "asset_one*",
    cron_schedule="* * * * *",  # runs every minute
)


defs = dg.Definitions(
    assets= [asset_one, asset_two, asset_three],
    schedules=[schedule_every_minute],
)
