from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from data_store import DataStore


def test_data_store() -> None:
    spark = SparkSession        \
        .builder                \
        .appName("data-store")    \
        .master("local[*]")     \
        .getOrCreate()

    data_store = DataStore(spark)
    df = data_store.load_stock_raw("aapl")
    df.show()


#if __name__ == "__main__":
#    test_data_store()
