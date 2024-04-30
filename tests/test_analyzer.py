import unittest
from pyspark.sql.functions import col
from pyspark.testing.utils import assertDataFrameEqual
from datetime import date

from test_base import TestBase
from analyzer import Analyzer


class TestTranformation(TestBase):
    
    def test_max_close_single_year(self):
        sample_data = [
            {"date": date.fromisoformat("2020-01-01"), "open": 1.0, "close": 5.0},
            {"date": date.fromisoformat("2020-05-01"), "open": 1.0, "close": 1.0}
        ]
  
        original_df = self.spark.createDataFrame(sample_data)
        transformed_df = Analyzer.max_value_for_year(original_df)
        expected_data = [
            {"date": date.fromisoformat("2020-01-01"), "open": 1.0, "close": 5.0}
        ]

        expected_df = self.spark.createDataFrame(expected_data)
        assertDataFrameEqual(transformed_df, expected_df)


    def test_max_close_equal(self):
        sample_data = [
            {"date": date.fromisoformat("2020-01-01"), "open": 1.0, "close": 5.0},
            {"date": date.fromisoformat("2020-05-01"), "open": 1.0, "close": 5.0}
        ]
  
        original_df = self.spark.createDataFrame(sample_data)
        transformed_df = Analyzer.max_value_for_year(original_df)
        expected_data = [
            {"date": date.fromisoformat("2020-01-01"), "open": 1.0, "close": 5.0}
        ]

        expected_df = self.spark.createDataFrame(expected_data)
        assertDataFrameEqual(transformed_df, expected_df)




if __name__ == "__main__":
    unittest.main()