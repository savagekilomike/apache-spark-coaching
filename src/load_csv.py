from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *

spark = SparkSession        \
    .builder                \
    .appName("load-csv")    \
    .master("local[*]")     \
    .getOrCreate()


def load_stock_data(symbol: str):
    df = spark.read         \
        .option("header", True) \
        .csv(f"data/{symbol}.csv")
#        .csv(f"data/stocks/{symbol}.csv")    

    return df.select(
        df['Date'].cast(DateType()).alias('date'),
        df['Open'].cast(DoubleType()).alias('open'),
        df['Close'].cast(DoubleType()).alias('close'),
        df['High'].cast(DoubleType()).alias('high'),
        df['Low'].cast(DoubleType()).alias('low')
    )


# df = load_stock_data(symbol=input("Symbol: "))
print("Loadind Stock Data... \n")
df = load_stock_data(symbol='AAPL')
print("Stock Data is loaded \n")
df.show()


