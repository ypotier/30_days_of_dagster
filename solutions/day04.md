# Day 4 

https://youtu.be/Jaann2QiGwQ

The solution to day 4 is to use Dagster's `RetryPolicy`. Retry po licies make it possible for a data pipeline to be robust to transient errors such as flaky APIs, network hiccups, or temporary cpu/memory limitations. This pipeline introduces a simple error and retry, but you can use more complex retry policies (such as backoff), or play around with adding retry policies in other places in the pipeline.

```python
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
```
