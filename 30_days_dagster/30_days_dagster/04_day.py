#prompt  Run asset A every minute, asset C every 10 minutes, and asset B only when it needs to be run by C.
import dagster as dg
import random

@dg.asset(
        automation_condition=dg.AutomationCondition.on_cron("* * * * *")
)
def asset_one(context: dg.AssetExecutionContext) -> None:
    random_choice = random.random()
    context.log.info(f"Random choice: {random_choice}")
    if random_choice < 0.5:
        raise dg.Failure(
            description="Asset one failed",
            metadata={
                "filepath": dg.MetadataValue.path("some_path/to_file"),
                "dashboard_url": dg.MetadataValue.url("http://mycoolsite.com/failures"),
            },
        )
    context.log.info("Creating asset one")

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