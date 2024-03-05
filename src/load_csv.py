from pyspark.sql import SparkSession


spark = SparkSession        \
    .builder                \
    .appName("load-csv")    \
    .master("local[*]")     \
    .getOrCreate()

df = spark.read             \
    .option("header", True) \
    .option("inferSchema", True)    \
    .csv("data/AAPL.csv")

df.show(10)
df.printSchema()