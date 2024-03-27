from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from utils import to_snake_case, all_columns_to_snake_case


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
        path = f"data/raw/{symbol.upper()}.csv"
        df_raw = self.spark.read                 \
            .option("header", True)         \
            .csv(path, schema=self.schema)  \
        
#       rename columns -> lower_snake_case
        df_cleaned = all_columns_to_snake_case(df_raw)

        return df_cleaned
    


# =======================================================
