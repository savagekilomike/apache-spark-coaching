from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
import argparse

from data_store import DataStore
from app_context import AppContext


class PrepareStep:

    def __init__(self, app_context: AppContext, data_store: DataStore) -> None:
        self.spark = app_context.spark
        self.config = app_context.config
        self.data_store = data_store

    def run(self) -> None:  
        symbols = self.config["prepare"]["symbols"]

        for symbol in symbols:
            df = self.data_store.load_stock_raw(symbol)
            self.data_store.save_stock_cln(symbol, df)


if __name__ == "__main__":
#    parser = argparse.ArgumentParser(description='Process configuration file for Spark application.')
#    parser.add_argument('config_file', type=str, help='Path to the configuration file')
#    args = parser.parse_args()
    
    #app_context = AppContext(args.config_file)
    app_context = AppContext()

    data_store = DataStore(app_context)
    prepare_step = PrepareStep(app_context, data_store)
    prepare_step.run()
