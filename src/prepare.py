# Prepare data
#   - Load raw CSV: 
#       - adjust data types
#       - rename columns
#   - Export DataFrame to Parquet, save to data/cln

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from data_store import DataStore
import json

class PrepareStep:

    def __init__(self, spark: SparkSession, data_store: DataStore) -> None:
        self.spark = spark
        self.data_store = data_store

    def run(self) -> None:
        """
        df_config = self.spark.read.json("src/config.json")
        """
        """
                df_config = self.spark.read \
                    .format("json") \
                    .load("src/config.json")
        """
        #df_config.show()
        
        
        with open("src/config.json", "r") as config_file:
            config = json.load(config_file)
        symbols = config["symbols"]
        # symbols = ["aapl"]
        # symbols = ["aapl", "abb", "abc", "ba", "band"]
        

        for symbol in symbols:
            df = self.data_store.load_stock_raw(symbol)
            df.show()
            self.data_store.store_stock_cln(symbol, df)