from spark_job_functions import filter_invalid_nutrient_amounts
from spark_job_functions import food_tables_id_to_int
from spark_job_functions import summarize

import unittest

import sys
import os
import pandas as pd
import pyspark
import findspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import lower, col
from pyspark.sql.functions import when
from pyspark.sql.functions import count, lit, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# RUN USING python -m unittest unit-testing-spark-job.py

class PySparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        conf = pyspark.SparkConf().setMaster("local[2]").setAppName("testing")
        cls.sc = pyspark.SparkContext.getOrCreate(conf=conf)
        cls.spark = pyspark.SQLContext(cls.sc)


class TestFilterInvalidNutrientAmounts(PySparkTestCase):
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
        result = filter_invalid_nutrient_amounts(self.df)
        self.assertTrue(result.isEmpty())

    def test_grams_amount_under_100(self):
        self.data = [(1111111,"Nutrient1",99.9,"G")]
        self.df = self.spark.createDataFrame(data=self.data, schema=self.schema)
        result = filter_invalid_nutrient_amounts(self.df)
        self.assertFalse(result.isEmpty())

    def test_milligrams_amount_over_100000(self):
        self.data = [(1111111,"Nutrient1",100000.1,"MG")]
        self.df = self.spark.createDataFrame(data=self.data, schema=self.schema)
        result = filter_invalid_nutrient_amounts(self.df)
        self.assertTrue(result.isEmpty())

    def test_milligrams_amount_under_100000(self):
        self.data = [(1111111,"Nutrient1",99999.9,"MG")]
        self.df = self.spark.createDataFrame(data=self.data, schema=self.schema)
        result = filter_invalid_nutrient_amounts(self.df)
        self.assertFalse(result.isEmpty())

    def test_amount_other_units(self):
        self.data = [(1111111,"Nutrient1",1000000.0,"IU")]
        self.df = self.spark.createDataFrame(data=self.data, schema=self.schema)
        result = filter_invalid_nutrient_amounts(self.df)
        self.assertFalse(result.isEmpty())

class TestFoodTablesIdToInt(PySparkTestCase):
    def setUp(self):
        self.schema = StructType([ \
            StructField("fdc_id", StringType(), True)
            ])

    def test_id_is_int(self):
        self.data = [("1111111",)]
        self.df = self.spark.createDataFrame(data=self.data,schema=self.schema)
        food = food_tables_id_to_int(self.df)
        self.assertFalse(food.isEmpty())

    def test_id_not_int(self):
        self.data = [("111111a",)]
        self.df = self.spark.createDataFrame(data=self.data,schema=self.schema)
        food = food_tables_id_to_int(self.df)
        self.assertTrue(food.isEmpty())

class TestSummarize(PySparkTestCase):
    def setUp(self):
        self.schema = StructType([ \
            StructField("name", StringType(), True), \
            StructField("amount", DoubleType(), True), \
            StructField("branded_food_category", StringType(), True) \
            ])
        self.row = Row('name', 'branded_food_category', 'count(1)', 'avg(amount)')
    
    def test_same_nutrient_same_category(self):
        self.data = [("Sugar", 15.0, "CEREAL"),
                     ("Sugar", 5.0, "CEREAL")]
        self.df = self.spark.createDataFrame(data=self.data, schema=self.schema)
        actual = summarize(self.df).collect()
        expected = [self.row('Sugar', 'CEREAL', 2, 10.0)]
        self.assertEqual(actual, expected)

    def test_same_nutrient_diff_category(self):
        self.data = [("Sugar", 15.0, "CEREAL"),
                     ("Sugar", 5.0, "BREAD")]
        self.df = self.spark.createDataFrame(data=self.data, schema=self.schema)
        actual = summarize(self.df).collect()
        expected = [self.row('Sugar', 'CEREAL', 1, 15.0),
                    self.row('Sugar', 'BREAD', 1, 5.0)]
        self.assertEqual(actual, expected)

    def test_diff_nutrient_same_category(self):
        self.data = [("Carbs", 15.0, "CEREAL"),
                     ("Sugar", 5.0, "CEREAL")]
        self.df = self.spark.createDataFrame(data=self.data, schema=self.schema)
        actual = summarize(self.df).collect()
        expected = [self.row('Carbs', 'CEREAL', 1, 15.0),
                    self.row('Sugar', 'CEREAL', 1, 5.0)]
        self.assertEqual(actual, expected)

    def test_diff_nutrient_diff_category(self):
        self.data = [("Carbs", 15.0, "CEREAL"),
                     ("Sugar", 5.0, "BREAD")]
        self.df = self.spark.createDataFrame(data=self.data, schema=self.schema)
        actual = summarize(self.df).collect()
        expected = [self.row('Carbs', 'CEREAL', 1, 15.0),
                    self.row('Sugar', 'BREAD', 1, 5.0)]
        self.assertEqual(actual, expected)


if __name__ == '__main__':
    unittest.main()
    pyspark.SparkContext.stop()

# https://stackoverflow.com/questions/33811882/how-do-i-unit-test-pyspark-programs
# https://stackoverflow.com/questions/23667610/what-is-the-difference-between-setup-and-setupclass-in-python-unittest