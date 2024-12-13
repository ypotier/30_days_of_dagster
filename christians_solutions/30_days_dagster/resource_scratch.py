import dagster as dg
import os 
import pandas as pd

class CsvStorageResource(dg.ConfigurableResource):
    base_dir: str = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data",
    )

    def read_data(self, csv_file):
        return pd.read_csv(os.path.join(self.base_dir, csv_file) )
    
    def write_data(self, df, csv_file):
        return df.to_csv(os.path.join(self.base_dir, csv_file), index=False )
    
class CsvAssetConfig(dg.Config):
    csv_file: str
