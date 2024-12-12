import os
import pandas as pd
from dagster_pipes import PipesContext, open_dagster_pipes

def main():
    # get the Dagster Pipes context
    context = PipesContext.get()
    # use the file path from the Dagster resource
    data_files_path = context.extras.get("data_files_path")
    context.log.info(f"loading data from {data_files_path}")
    orders = pd.read_csv(os.path.join(data_files_path, "orders_raw.csv") )
    # logs back to Dagster
    context.log.info(f"Creating asset one with data: {orders.head()}")
    # directly write csv to storage
    orders.to_csv(os.path.join(data_files_path, "orders.csv") )
    # report asset materialization metadata back to the main process
    context.report_asset_materialization(
        metadata={
            "dagster/row_count": len(orders), 
            "preview": orders.head().to_markdown(),
            "orders_df_head": orders.head().to_json(),
        }
    )


if __name__ == "__main__":
    # connect to Dagster Pipes
    with open_dagster_pipes():
        main()
