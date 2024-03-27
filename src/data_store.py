from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from utils import to_snake_case, all_columns_to_snake_case


class DataStore:

    schema = StructType([
        StructField("Date", DateType(), False),
        StructField("Open", DoubleType(), False),
        StructField("Close", DoubleType(), False),
        StructField("High", DoubleType(), False),
        StructField("Low", DoubleType(), False)
    ])

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def load_stock_raw(self, symbol: str) -> DataFrame:
        path = f"data/raw/{symbol.upper()}.csv"
        df_raw = spark.read                 \
            .option("header", True)     \
            .csv(path, schema=self.schema)   \
        
#        rename columns -> lower_snake_case

# 1 - alias       
#        df_cleaned = df_raw.select(
#            df_raw['Date'].cast(DateType()).alias('date'),
#            df_raw['Open'].cast(DoubleType()).alias('open'),
#            df_raw['Close'].cast(DoubleType()).alias('close'),
#            df_raw['High'].cast(DoubleType()).alias('high'),
#            df_raw['Low'].cast(DoubleType()).alias('low')
#        )
               
# 2 - alias + to_snake_case
#        df_cleaned = df_raw.select(        
#            df_raw['Date'].cast(DateType()).alias(to_snake_case(df_raw.columns[0])),
#            df_raw['Open'].cast(DoubleType()).alias(to_snake_case(df_raw.columns[1])),
#            df_raw['Close'].cast(DoubleType()).alias(to_snake_case(df_raw.columns[2])),
#            df_raw['High'].cast(DoubleType()).alias(to_snake_case(df_raw.columns[3])),
#            df_raw['Low'].cast(DoubleType()).alias(to_snake_case(df_raw.columns[4]))            
#        )
        
# 3 - withColumnRenamed
        df_cleaned = df_raw.select(        
            df_raw['Date'].cast(DateType()),
            df_raw['Open'].cast(DoubleType()),
            df_raw['Close'].cast(DoubleType()),
            df_raw['High'].cast(DoubleType()),
            df_raw['Low'].cast(DoubleType())
        )

        df_cleaned = all_columns_to_snake_case(df_cleaned)

        return df_cleaned
    


# =======================================================
spark = SparkSession        \
    .builder                \
    .appName("data-store")    \
    .master("local[*]")     \
    .getOrCreate()

data_store = DataStore(spark)
df = data_store.load_stock_raw("aapl")
df.show()