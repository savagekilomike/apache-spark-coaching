import datetime
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.types import *
from pyspark.sql.functions import date_format, date_add, year, month, max, avg, sum, col, when

pyspark.sql.Column.when()


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
    .add("date", DateType(), True)       \
    .add("open", DoubleType(), True)     \
    .add("close", DoubleType(), True)

df = spark.createDataFrame([
    Row(None, None, 2.0),
    Row(datetime.date(2023, 12, 2), 2.0, 3.0),
    Row(datetime.date(2023, 12, 3), 3.0, None)
], schema)

#df.na.fill({"date": "1899-12-31", "open": 0.0, "close": 1.0}).show()
#df.na.fill({"date": str(datetime.date(9999, 12, 31)), "open": 0.0, "close": 1.0}).show()

default_Value = datetime.date(9999, 12, 31)
df.withColumn("date", when(df["date"].isNull(), default_Value).otherwise(df["date"])).show()
# didn't work without > from pyspark.sql.functions import when
# but > DataFrame.withColumn(colName: str, col: pyspark.sql.column.Column) → pyspark.sql.dataframe.DataFrame
# pyspark.sql.Column.when()
# doesn't work with > from pyspark.sql import Column


