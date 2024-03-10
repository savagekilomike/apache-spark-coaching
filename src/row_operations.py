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


df = load_stock_data("AAPL")
#df.show(n=10, truncate=False)


# SORTING
df.sort("date").show()
df.sort(df["date"].desc()).show()
df.sort(date_add(df["date"], 2).desc()).show()
df.sort(df["date"].desc(), df["close"])


# GROUPING + SORTING
df.groupby("date").max("close").show()
df.groupby(
            year(df['date']).alias("year"),                 \
            month(df['date']).alias("month"),               \
#            date_format(df['date'], 'MMM').alias("month")   \
           )                                                \
            .agg(                                       \
                    max("close").alias("max_close"),    \
                    avg("close").alias("avg_close"),    \
                )                                       \
            .sort(col("max_close").desc())              \
            .show()
#            .sort(max("close").desc())                 \
