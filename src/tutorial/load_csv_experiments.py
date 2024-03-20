from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *

spark = SparkSession        \
    .builder                \
    .appName("load-csv")    \
    .master("local[*]")     \
    .getOrCreate()

df = spark.read             \
    .option("header", True) \
    .option("inferSchema", True)    \
    .csv("data/AAPL.csv")

# df.show(10)
# df.printSchema()
# df.summary().show()

# df.Volume
# df['Date']
# col('Open')
# "column_name"

date = df['Date']
open = df['Open']
close = df['Close']
high = df['High']
low = df['Low']

df.select(date, open, close, high, low).show()
df.select("Date", "Open", "Close", "High", "Low").show()

open_close = df['Open', 'Close']

 
df_date = df.select(df['Date'].cast(DateType()), "Open", "Close", "High", "Low")
print("ISO Date")
df_date.show()

date_as_string = df['Date'].cast(StringType()).alias()