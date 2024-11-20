from dagster import asset   

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