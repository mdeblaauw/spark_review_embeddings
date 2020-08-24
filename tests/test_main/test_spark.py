import unittest
import pandas as pd
from pyspark.sql import SparkSession
from spark_main.main import main


class TestSparkMain(unittest.TestCase):
    @classmethod
    def create_testing_pyspark_session(cls):
        return (SparkSession.builder
            .master('local[2]')
            .appName("my-local-testing-pyspark-context")
            .enableHiveSupport()
            .getOrCreate())

    @classmethod
    def setUpClass(cls):
        cls.spark = cls.create_testing_pyspark_session()
        cls.df = cls.spark.read.json(
            'tests/mockdata/sample_10_yelp_reviews.json'
        )

    def test_test(self):
        df = main(TestSparkMain.df).toPandas()
        print(df)

    @classmethod
    def tearDown(cls):
        pass