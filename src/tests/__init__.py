import unittest

from pyspark.sql import SparkSession

SPARK_SESSION: SparkSession = (SparkSession.builder
                               .master('local[*]')
                               .appName('PySpark-unit-test')
                               .config("spark.sql.shuffle.partitions", "20")
                               .config('spark.port.maxRetries', 10)
                               .getOrCreate())

if __name__ == '__main__':
    unittest.main()
    # SPARK_SESSION.stop()
