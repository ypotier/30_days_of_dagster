# Day 1 Solution 

First install the necessary packages:

```bash
pip install dagster dagster-webserver
```

Create a Python file called `definitions.py` with the code:

```python
import dagster as dg 

@dg.asset
def a():
    ...

@dg.asset(
    deps=[a]
)
def b(context: dg.AssetExecutionContext):
    context.log.info("Hello")

@dg.asset(
    deps=[b]
)
def c(context: dg.AssetExecutionContext):
    context.log.info("World")
```

Start the dagster webserver by running:

```bash
dagster dev
```

Select `Materialize All`
