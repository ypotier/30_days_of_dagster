from dagster import asset, schedule, AssetSelection, SkipReason, RunRequest, Definitions

@asset 
def a(): ... 

@asset(
    deps=[a]
) 
def b(): ... 

@asset(
    deps=[b]
) 
def c(): ... 

@schedule(
    cron_schedule="* * * * *", 
    target=AssetSelection.assets("a", "b", "c")
)
def my_minutely_schedule():
    return RunRequest()

defs = Definitions(
    assets=[a, b, c],
    schedules=[my_minutely_schedule]
)