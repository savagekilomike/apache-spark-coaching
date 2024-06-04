from pathlib import Path

from pyspark.sql.functions import *

from app_context import AppContext
from data_store import DataStore


class PrepareStep:

    def __init__(self, app_context: AppContext, data_store: DataStore) -> None:
        self.spark = app_context.spark
        self.config = app_context.config
        self.data_store = data_store

    def run(self):
        symbols = self.config["prepare"]["symbols"]

        for symbol in symbols:
            df = self.data_store.load_stock_raw(symbol)
            self.data_store.save_stock_cln(symbol, df)


if __name__ == "__main__":
    if len(sys.argv) == 2:
        config_file = sys.argv[1]
    else:
        config_file = "config/config.json"

    app_context = AppContext(Path(config_file))
    data_store = DataStore(app_context)
    prepare_step = PrepareStep(app_context, data_store)
    prepare_step.run()
