
# Day 2 Solution 

https://youtu.be/Wja1EmyIexE

The key idea of they day 2 solution is using `@schedule` to create a schedule, specifying the assets for the schedule in `target`, and adding everything to a `Definitions` object.


```python 
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
```
`@schedule` is valuable because it allows you to handle complex scheduling situations independent from the actual job runs. For example: 

```python 
@schedule(
    cron_schedule="* * * * *",
    target=AssetSelection.all()
)
def run_daily_except_holidays(): 
    if today() in my_corporate_calendar_holidays():
        return SkipReason("Not running because today is a holiday")
    
    return RunRequest()
```