from dagster import asset, AssetExecutionContext, DailyPartitionsDefinition 

# https://youtu.be/_WSOaE5mr4g

days = DailyPartitionsDefinition(start_date="2024-12-10")

@asset(
    partitions_def=days
) 
def a(context: AssetExecutionContext):
    partition = context.partition_key
    context.log.info(partition)
    return 

@asset(
        deps=[a]
) 
def b(context: AssetExecutionContext):
    ...
