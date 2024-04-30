from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

from app_context import AppContext
from data_store import DataStore

class Analyzer:

    def __init__(self, app_context: AppContext, data_store: DataStore) -> None:
        self.spark = app_context.spark
        self.config = app_context.config
        self.data_store = data_store

    def run(self) -> None:  
        """ 
        Highest closing price per year: 
        - Should return the row in the input DataFrame of the day 
        which reported the highest closing price in the year.
        - If there are two days within the year which have the same 
        highest closing price, the one with the earlier date should be returned
        """
        max_close_agg = None

        symbols = self.config["analyze"]["symbols"]

        for symbol in symbols:
            symbol = symbol.upper()
            df = (self.data_store
                  .load_stock_cln(symbol)
                  .withColumn("symbol", lit(symbol))
                  )

            max_close_price = self.max_value_for_year(df)
            max_close_price.show()

            if max_close_agg is None:
                max_close_agg = max_close_price
            else:
                max_close_agg = max_close_agg.union(max_close_price)

        if max_close_agg is not None:
            self.data_store.save_drv("max_close_agg", max_close_agg)


    @staticmethod
    def max_value_for_year(df: DataFrame, column: str = "close") -> DataFrame:
        window_spec = (Window
            .partitionBy(year(col("date")))
            .orderBy(desc(column), "date"))
        
        df_ranked = (df
                     .withColumn("rank", rank().over(window_spec))
                     .drop("rank"))

        max_close_price = df_ranked.filter(col("rank") == 1)

        return max_close_price    
    

if __name__ == "__main__":
    app_context = AppContext()

    data_store = DataStore(app_context)
    analysis_step = Analyzer(app_context, data_store)
    analysis_step.run()
