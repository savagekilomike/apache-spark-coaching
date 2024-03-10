from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import date_format
from pyspark.sql.types import *

spark = SparkSession        \
    .builder                \
    .appName("load-csv")    \
    .master("local[*]")     \
    .getOrCreate()


def load_stock_data(symbol: str) -> DataFrame:
    df = spark.read                         \
        .option("header", True)             \
        .csv(f"data/{symbol}.csv")
#        .csv(f"data/stocks/{symbol}.csv")
    return df


def clean_stock_data(df: DataFrame)  -> DataFrame:
    df_cleaned = df.select(
        df['Date'].cast(DateType()).alias('date'),
        df['Open'].cast(DoubleType()).alias('open'),
        df['Close'].cast(DoubleType()).alias('close'),
        df['High'].cast(DoubleType()).alias('high'),
        df['Low'].cast(DoubleType()).alias('low')
    )
    return df_cleaned


def process_stock_data(df: DataFrame) -> DataFrame:
    df_processed = df.select(
        df['date'],
        date_format('date', 'GGG-yyy-MMM-D-EEEE').alias('reformatted_date'),
        (df['close'] - df['open']).alias('diff_open_close'),
        (df['high'] - df['low']).alias('diff_high_low')
    )
    return df_processed

# df = load_stock_data(symbol=input("Symbol: "))

# LOAD DATA
print("\n Loadind Stock Data... \n")
df_raw = load_stock_data(symbol='AAPL')
print("\n Stock Data is loaded \n")
df_raw.show(n=10, truncate=False)

# CLEAN DATA
print("\n Cleaning Stock Data... \n")
df_cleaned = clean_stock_data(df_raw)
print("\n Stock Data is cleaned \n")
df_cleaned.show(n=10, truncate=False)

# PROCESS DATA
print("\n Processing Stock Data... \n")
df_processed = process_stock_data(df_cleaned)
print("\n Stock Data is processed \n")
df_processed.show(n=10, truncate=False)


# SQL
df_raw.createOrReplaceTempView("df_raw")
spark.sql("select \
            date    \
            , cast(date_add(date, 2) as string) as transformed_date \
          from df_raw")     \
        .show(10)


df_raw.createOrReplaceTempView("df_processed")
spark.sql("select                               \
            cast(date as date) as date          \
            , cast(open as double) as open      \
            , cast(close as double) as close    \
            , cast(high as double) as high      \
            , cast(low as double) as low        \
          from df_processed")                   \
        .show(10)