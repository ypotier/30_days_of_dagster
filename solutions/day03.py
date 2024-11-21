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