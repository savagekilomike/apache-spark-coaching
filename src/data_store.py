from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from utils import all_columns_to_snake_case
from app_context import AppContext

class DataStore:

    schema = StructType([
        StructField("Date", DateType(), False),
        StructField("Open", DoubleType(), False),
        StructField("High", DoubleType(), False),
        StructField("Low", DoubleType(), False),
        StructField("Close", DoubleType(), False),
        StructField("Adj Close", DoubleType(), False),
        StructField("Volume", IntegerType(), False)
    ])


    def __init__(self, app_context: AppContext) -> None:
        self.spark = app_context.spark
        self.config = app_context.config


    def load_stock_raw(self, symbol: str) -> DataFrame:
        """ Read CSV, Rename columns """
        input_dir = self.config["prepare"]["input_dir"]
        path = f"{input_dir}{symbol.upper()}.csv"

        df = (self.spark.read
            .option("header", True)
            .csv(path, schema=self.schema))
        
        df = all_columns_to_snake_case(df)

        return df


    # rename the method
    def save_stock_cln(self, symbol: str, df: DataFrame) -> None:
        """ Export DataFrame to Parquet and store to data/cln/ """
        output_dir = self.config["prepare"]["output_dir"]
        path = f"{output_dir}{symbol.upper()}/"
        df.write.parquet(path=path, mode="overwrite")


    def load_stock_cln(self, symbol: str) -> DataFrame:
        """ Load Parquet file"""
        input_dir = self.config["analyze"]["input_dir"]
        path = f"{input_dir}{symbol.upper()}"
        df = self.spark.read.parquet(path)

        return df
    

    def save_drv(self, filename: str, df: DataFrame) -> DataFrame:
        """ Save derrived file to data/drv/"""
        output_dir = self.config["analyze"]["output_dir"]
        path = f"{output_dir}{filename}"
        df.write.parquet(path, mode="overwrite")


""" OLD Schema

    schema = StructType([
        StructField("Date", DateType(), False),
        StructField("Open", DoubleType(), False),
        StructField("Close", DoubleType(), False),
        StructField("High", DoubleType(), False),
        StructField("Low", DoubleType(), False)
    ])
"""