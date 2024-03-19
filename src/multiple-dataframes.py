import datetime
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.types import *
from pyspark.sql.functions import date_format, date_add, year, month, max, avg, sum, col, when


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
    .add("id", IntegerType(), True)       \
    .add("open", DoubleType(), True)     \
    .add("close", DoubleType(), True)

df1 = spark.createDataFrame([
    Row(id=1, open=1.0, close=1.0),
    Row(id=2, open=2.0, close=2.0),
    Row(id=3, open=3.0, close=3.0)
], schema)

df2 = spark.createDataFrame([
    Row(id=1, open=1.0, close=1.0),
    Row(id=2, open=2.0, close=2.0),
    Row(id=4, open=4.0, close=4.0)
], schema)


print("Inner Joins")
condition = (df1["id"] + 2) == df2["id"]

df1.join(df2, "id", "inner").show()
df1.join(df2, ["id", "close"], "inner").show()
df1.join(df2, condition, "inner").show()

print("Left Joins")
df1.join(df2, on="id", how="left").show()

print("Cross Joins")
# df1.join(df2, how="cross").show()
# df1.crossJoin(df2).show()


print("Unions")
df1.union(df2).show()       # no automatic deduplication of elements
df1.union(df2).distinct().show()

df1.unionAll(df2).show()    # no automatic deduplication of elements