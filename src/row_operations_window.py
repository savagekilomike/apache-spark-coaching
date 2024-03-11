from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import date_format, date_add, year, month, max, avg, sum, col, row_number, lag
from pyspark.sql import Window


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


df = load_stock_data("AAPL")


window = Window.partitionBy(year(df['date'])).orderBy(df['close'].desc(), df['date'])
df.withColumn("rank", row_number().over(window))    \
    .filter(col('rank') == 1)                       \
    .show()



window = Window.partitionBy().orderBy(df['date'])
df.withColumn("previous_day_close", lag(df['close'], 1, 0.0).over(window))      \
    .withColumn("diff_previous_close", df['open'] - col('previous_day_close'))  \
    .show()
