from dagster import (
    asset,
    AssetCheckResult,
    AssetKey,
    AssetCheckSeverity,
    AssetExecutionContext,
    Config,
    MetadataValue,
    ConfigurableResource,
    Definitions,
    asset_check,
)
import pandas as pd
from pydantic import Field
import shutil

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    PipesSubprocessClient,
    asset,
    file_relative_path,
)


class MyFilePath(Config):
    path: str = Field(
        default="solutions/bike_info.csv", description="The path to the csv to parse."
    )


class MyCSVToFile(ConfigurableResource):
    path: str

    def write(self, data: pd.DataFrame):
        data.to_csv(self.path)

    def read(self):
        return pd.read_csv(self.path)


@asset(
    owners=["lopp@dagsterlabs.com"],
    tags={"layer": "bronze"},
    kinds={"pandas", "csv"},
)
def bike_raw_cleaned(
    context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
):
    """Reads in the raw bike sales data from a local csv file. TODO: pull the file from the web dynamically"""

    cmd = [shutil.which("python"), file_relative_path(__file__, "external_code.py")]

    return pipes_subprocess_client.run(
        command=cmd, context=context
    ).get_materialize_result()


@asset_check(asset=bike_raw_cleaned, blocking=True)
def check_bike_raw_cleaned(csv_file_resource: MyCSVToFile):
    bike_raw_cleaned: pd.DataFrame = csv_file_resource.read()

    return AssetCheckResult(
        severity=AssetCheckSeverity.ERROR,
        passed=len(bike_raw_cleaned) > 0,
        metadata={"nrow": len(bike_raw_cleaned)},
    )


@asset(deps=[bike_raw_cleaned])
def bikes_summary(context: AssetExecutionContext, csv_file_resource: MyCSVToFile):
    bike_raw_cleaned: pd.DataFrame = (
        csv_file_resource.read()
    )  # pd.read_csv("bike_raw_cleaned.csv")

    context.log.info(bike_raw_cleaned)
    bike_summary = bike_raw_cleaned.groupby("type")["price"].mean()
    print(bike_summary)
    return


@asset(deps=[bikes_summary])
def c(): ...


defs = Definitions(
    assets=[bike_raw_cleaned, bikes_summary],
    asset_checks=[check_bike_raw_cleaned],
    resources={
        "csv_file_resource": MyCSVToFile(path="bike_raw_cleaned.csv"),
        "pipes_subprocess_client": PipesSubprocessClient(),
    },
)
