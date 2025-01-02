from dagster import asset, Definitions, RunConfig

from dagster_dbt import build_schedule_from_dbt_selection 


# https://youtu.be/azfCDBKTIoQ

@asset 
def a(): ...

def asset_factory(asset_name):
    @asset(
            deps=[a],
            name=asset_name
    ) 
    def my_asset(): 
        ...

    return my_asset

my_assets = [asset_factory(str(i)) for i in range(1,10)]

defs = Definitions(
    assets=[a] + my_assets
)