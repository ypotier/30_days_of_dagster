from dagster import asset, RetryPolicy  
import random

@asset(
    retry_policy=RetryPolicy(max_retries=4)
) 
def a(): 
    if random.randint(1,2) > 1:
        raise Exception()
    return

@asset(
    deps=[a]
) 
def b(): ... 

@asset(
    deps=[b]
) 
def c(): ...