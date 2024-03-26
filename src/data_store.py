from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *


class DataStore:

    schema = StructType([
        StructField("date", DateType(), False),
        StructField("open", DoubleType(), False),
        StructField("Close", DoubleType(), False),
        StructField("High", DoubleType(), False),
        StructField("Low", DoubleType(), False)
    ])

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def load_stock_raw(self, symbol: str) -> DataFrame:
        path = f"data/raw/{symbol.upper()}.csv"
        df_raw = spark.read                 \
            .option("header", True)     \
            .csv(path, schema=self.schema)   \
        
        # rename columns -> lower_snake_case
        df_cleaned = df_raw.select(
            df_raw['Date'].cast(DateType()).alias('date'),
            df_raw['Open'].cast(DoubleType()).alias('open'),
            df_raw['Close'].cast(DoubleType()).alias('close'),
            df_raw['High'].cast(DoubleType()).alias('high'),
            df_raw['Low'].cast(DoubleType()).alias('low')
        )

        return df_cleaned
# -----------------------------------------------------------------------------------------------------

spark = SparkSession        \
    .builder                \
    .appName("data-store")    \
    .master("local[*]")     \
    .getOrCreate()

ds = DataStore(spark)
df = ds.load_stock_raw("aapl")
df.show()