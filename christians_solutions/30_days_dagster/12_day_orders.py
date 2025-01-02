# external python script
import os
import pandas as pd
from dagster_pipes import PipesContext, open_dagster_pipes
from dagster_pandas.data_frame import create_table_schema_metadata_from_dataframe



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
    context.report_custom_message(payload = {
            "dagster/row_count": len(orders), 
            "preview": orders.head().to_markdown(),
            # "preview" : { "type": "md", "raw_value": orders.head().to_markdown() },
            # "dagster/column_schema": {"type": "table", "raw_value": create_table_schema_metadata_from_dataframe(orders.head()) },
            "orders_df_head": orders.head().to_json(),
        })


if __name__ == "__main__":
    # connect to Dagster Pipes
    with open_dagster_pipes():
        main()
