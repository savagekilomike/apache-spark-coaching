from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("hello-world").master("local[*]").getOrCreate()

print(f"Hello Apache Spark {spark.version}")