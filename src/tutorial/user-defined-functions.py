import datetime
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.types import *
from pyspark.sql.functions import *

@udf()
def days_between(date_1: datetime.date, date_2: datetime.date) -> IntegerType:
    return (date_1 - date_2).days

#udf_days_between = udf(days_between, returnType=IntegerType())


spark = SparkSession        \
        .builder            \
        .appName("udfs")    \
        .master("local[*]")         \
        .getOrCreate()

schema = StructType()   \
    .add("date", DateType(), False)       \
    .add("open", DoubleType(), False)     \
    .add("close", DoubleType(), False)

df1 = spark.createDataFrame([
    Row(date=datetime.date(2023, 12, 1), open=1.0, close=1.0),
    Row(date=datetime.date(2023, 12, 2), open=2.0, close=2.0),
    Row(date=datetime.date(2023, 12, 3), open=3.0, close=3.0)
], schema)


reference_date = lit(datetime.date(1991, 6, 13))

#df1.withColumn("days_between", udf_days_between(df1["date"], reference_date)).show()
df1.withColumn("days_between", days_between(df1["date"], reference_date)).show()