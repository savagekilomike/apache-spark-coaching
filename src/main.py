from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from data_store import DataStore
from prepare import PrepareStep


def prepare_data() -> None:
    spark = SparkSession        \
        .builder                \
        .appName("prepare-data")    \
        .master("local[*]")     \
        .getOrCreate()

    data_store = DataStore(spark)
    prepare_step = PrepareStep(spark, data_store)
    prepare_step.run()

if __name__ == "__main__":
    prepare_data()
