import unittest
from pyspark.sql import SparkSession


class TestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Test Analyzer").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()