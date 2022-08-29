from spark_job_functions import filter_invalid_nutrient_amounts
import unittest

import sys
import os
import pandas as pd
import pyspark
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col
from pyspark.sql.functions import when
from pyspark.sql.functions import count, lit, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# RUN USING python -m unittest unit-testing-spark-job.py

findspark.init()

class TestFilterInvalidNutrientAmounts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        conf = pyspark.SparkConf().setMaster("local[2]").setAppName("testing")
        cls.sc = pyspark.SparkContext(conf=conf)
        cls.spark = pyspark.SQLContext(cls.sc)

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()

    def setUp(self):
        self.schema = StructType([ \
            StructField("fdc_id", IntegerType(), True), \
            StructField("name", StringType(), True), \
            StructField("amount", DoubleType(), True), \
            StructField("unit_name", StringType(), True) \
            ])

    def test_grams_amount_over_100(self):
        self.data = [(1111111,"Nutrient1",100.1,"G")]
        self.df = self.spark.createDataFrame(data=self.data, schema=self.schema)
        self.assertTrue(filter_invalid_nutrient_amounts(self.df).isEmpty())

    def test_grams_amount_under_100(self):
        self.data = [(1111111,"Nutrient1",99.9,"G")]
        self.df = self.spark.createDataFrame(data=self.data, schema=self.schema)
        result_df = filter_invalid_nutrient_amounts(self.df)
        fdc_id_arr = [int(row["fdc_id"]) for row in result_df.collect()]
        self.assertTrue(set(fdc_id_arr) == {1111111})

    def test_milligrams_amount_over_100000(self):
        self.data = [(1111111,"Nutrient1",100000.1,"MG")]
        self.df = self.spark.createDataFrame(data=self.data, schema=self.schema)
        self.assertTrue(filter_invalid_nutrient_amounts(self.df).isEmpty())

    def test_milligrams_amount_under_100000(self):
        self.data = [(1111111,"Nutrient1",99999.9,"MG")]
        self.df = self.spark.createDataFrame(data=self.data, schema=self.schema)
        result_df = filter_invalid_nutrient_amounts(self.df)
        fdc_id_arr = [int(row["fdc_id"]) for row in result_df.collect()]
        self.assertTrue(set(fdc_id_arr) == {1111111})

    def test_amount_other_units(self):
        self.data = [(1111111,"Nutrient1",1000000.0,"IU")]
        self.df = self.spark.createDataFrame(data=self.data, schema=self.schema)
        result_df = filter_invalid_nutrient_amounts(self.df)
        fdc_id_arr = [int(row["fdc_id"]) for row in result_df.collect()]
        self.assertTrue(set(fdc_id_arr) == {1111111})


if __name__ == '__main__':
    unittest.main()


# https://stackoverflow.com/questions/33811882/how-do-i-unit-test-pyspark-programs
# https://stackoverflow.com/questions/23667610/what-is-the-difference-between-setup-and-setupclass-in-python-unittest