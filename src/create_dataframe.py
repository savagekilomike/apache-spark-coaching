import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import date_format, date_add, year, month, max, avg, sum, col

spark = SparkSession        \
        .builder            \
        .appName("process-rows")    \
        .master("local[*]")         \
        .getOrCreate()


def load_stock_data(symbol: str) -> DataFrame:
    df = spark.read                 \
        .option("header", True)     \
        .csv(f"data/{symbol}.csv")

    return df.select(
        df['Date'].cast(DateType()).alias('date'),
        df['Open'].cast(DoubleType()).alias('open'),
        df['Close'].cast(DoubleType()).alias('close'),
        df['High'].cast(DoubleType()).alias('high'),
        df['Low'].cast(DoubleType()).alias('low')
    )

schema = StructType()   \
    .add("date", DateType(), False)       \
    .add("open", DoubleType(), False)     \
    .add("close", DoubleType(), False)

df = spark.createDataFrame([
    Row(datetime.date(2023, 12, 1), 1.0, 2.0),
    Row(datetime.date(2023, 12, 2), 2.0, 3.0),
    Row(datetime.date(2023, 12, 3), 3.0, 1.0)
], schema)

df.show()