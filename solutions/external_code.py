import pandas as pd
from dagster_pipes import PipesContext, open_dagster_pipes

with open_dagster_pipes():
    context = PipesContext.get()
    df = pd.read_csv("solutions/bike_info.csv")
    df.columns = df.columns=['index','junk', 'bike', 'msrp', 'price', 'brand','category','type','details', 'junk2']
    df = df.drop(['index', 'junk', 'junk2', 'details'], axis=1)
    bikes = df.dropna()
    bikes['price'] = bikes['price'].str.replace(',', '')
    bikes['price'] = bikes['price'].str.replace('$', '').astype(float)
    nrow = len(bikes)

    context.log.info(f"Parsed a csv file and now have {nrow}")
    context.report_asset_materialization({
        "rows": nrow,
        "head": str(bikes.head().to_markdown()),
        "columns": str(bikes.columns)
    })

    bikes.to_csv("bike_raw_cleaned.csv")