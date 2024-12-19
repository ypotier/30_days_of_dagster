from dagster import multi_asset, AssetSpec

# https://youtu.be/mrlFdKJlosY

@multi_asset(
        specs=[AssetSpec('a'), AssetSpec('b')]
)
def my_expensive_operation(): ...
