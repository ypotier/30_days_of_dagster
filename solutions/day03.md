# Day 3 Solution 

https://youtu.be/xUJTal1cq0Y 

The key to today's challenge is using `AutomationCondition` which allows each asset to declare its own policy for when it should run. 

```python 
from dagster import asset, AutomationCondition   

@asset(
        automation_condition=AutomationCondition.on_cron("* * * * *")
) 
def a(): ... 

@asset(
    deps=[a], 
    automation_condition=AutomationCondition.any_downstream_conditions()
) 
def b(): ... 

@asset(
    deps=[b],
    automation_condition=AutomationCondition.on_cron("*/10 * * * *")
) 
def c(): ... 
```