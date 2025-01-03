from dagster import asset, schedule, AssetSelection, SkipReason, RunRequest, Definitions, AssetIn


@asset
def step_a() -> str:
    """First step in our pipeline"""
    return "Output from step A"


@asset(ins={'a_output': AssetIn('step_a')})
def step_b(a_output: str) -> str:
    """Second step that depends on step A"""
    return f"Step B processed: {a_output}"


@asset(ins={'b_output': AssetIn('step_b')})
def step_c(b_output: str) -> str:
    """Third step that depends on step B"""
    return f"Step C processed: {b_output}"


"""A minutely schedule that triggers the execution of step_a, step_b, and step_c.

This schedule runs every minute and initiates the execution of three assets:
step_a, step_b, and step_c.
"""


@schedule(
    cron_schedule="* * * * *",
    target=AssetSelection.assets("step_a", "step_b", "step_c")
)
def my_minute_schedule():
    return RunRequest()


defs = Definitions(
    assets=[step_a, step_b, step_c],
    schedules=[my_minute_schedule]
)
