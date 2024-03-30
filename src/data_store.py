from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from utils import to_snake_case, all_columns_to_snake_case
import json

class DataStore:

    schema = StructType([
        StructField("Date", DateType(), False),
        StructField("Open", DoubleType(), False),
        StructField("Close", DoubleType(), False),
        StructField("High", DoubleType(), False),
        StructField("Low", DoubleType(), False)
    ])


    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark


    def load_stock_raw(self, symbol: str) -> DataFrame:
        """ Read CSV, Rename columns """
        with open("src/config.json", "r") as config_file:
            config = json.load(config_file)
        input_dir = config["input_dir"]
        path = f"{input_dir}{symbol.upper()}.csv"

        df = self.spark.read                 \
            .option("header", True)         \
            .csv(path, schema=self.schema)  \
        
        df = all_columns_to_snake_case(df)

        return df


    def store_stock_cln(self, symbol: str, df: DataFrame) -> None:
        """ Export DataFrame to Parquet and store to data/cln/ """
        with open("src/config.json", "r") as config_file:
            config = json.load(config_file)
        output_dir = config["input_dir"]

        path = f"{output_dir}{symbol.upper()}/"
        df.write.parquet(path=path, mode="overwrite")