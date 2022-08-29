import sys
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col
from pyspark.sql.functions import when
from pyspark.sql.functions import count, lit, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Fn called by MAIN
def filter_invalid_nutrient_amounts(nutrient_amounts): 
    nutrient_amounts = nutrient_amounts.withColumn("keep_row", 
            when((nutrient_amounts.amount >= 100) & (nutrient_amounts.unit_name == "G"), False)
            .when((nutrient_amounts.amount >= 100000) & (nutrient_amounts.unit_name == "MG"), False)
            .otherwise(True)) \
        .filter(col("keep_row") == True) \
        .drop("keep_row") \
        .orderBy("amount", ascending=False)
    
    return nutrient_amounts

