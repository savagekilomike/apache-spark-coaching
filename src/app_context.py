import json
from pathlib import Path

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


class AppContext:

    def __init__(self, config_filename: Path = Path("config/config.json")) -> None:
        self.config = self.load_conf(config_filename)
        self.spark = self.init_spark()

    @staticmethod
    def load_conf(conf_file: Path):
        with open(conf_file, 'r') as f:
            config = json.load(f)
        return config

    def init_spark(self) -> SparkSession:
        spark_conf = SparkConf()
        for (k, v) in self.config['spark'].items():
            spark_conf.set(f"spark.{k}", v)

        print(spark_conf.toDebugString())

        return SparkSession \
            .builder \
            .config(conf=spark_conf) \
            .getOrCreate()
