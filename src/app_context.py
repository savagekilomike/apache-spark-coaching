from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json
from pyspark.conf import SparkConf


class AppContext:
    
    def __init__(self, config_filename: Path = "config/config.json") -> None:

        with open(config_filename, 'r') as config_file:
            self.config = json.load(config_file)
        
        spark_conf = SparkConf()
        for k, v in self.config['spark_app'].items():
            spark_conf.set(f"spark.{k}", v)            

        self.spark = (SparkSession.builder
            .config(conf=spark_conf)
            .getOrCreate()
            )
        