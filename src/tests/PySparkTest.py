
import unittest

from pyspark.sql import DataFrame

from src.tests import SPARK_SESSION


class PySparkTest(unittest.TestCase):
    """ This class helps to run all unit tests with utilities to compare dataframes"""

    @classmethod
    def setUpClass(cls):
        cls.spark = SPARK_SESSION

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


    @classmethod
    def df_of(cls, records: list, schema=None) -> DataFrame:
        return cls.spark.createDataFrame(records, schema=schema)

    def assertDataFrameEqual(self, expected: DataFrame, actual: DataFrame):
        self.assertEqual(expected.collect(), actual.collect())
        self.assertListEqual(expected.schema.names, actual.schema.names)
