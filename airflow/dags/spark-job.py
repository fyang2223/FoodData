import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col

AIRHOME = sys.argv[1]
CSVFOLDER = sys.argv[2]







if __name__ == '__main__':
    spark = SparkSession.builder.master("spark://spark:7077").appName("NutrientAnalysis").getOrCreate()

    pwd = os.system("pwd")
    # tablename = "hello"
    # food_category = spark.read.csv(f"{AIRHOME}/dags/testfile.txt", header=True, inferSchema=True)

    tablename = "food_category"
    food_category = spark.read.csv(f"hdfs://namenode:9000/data/openbeer/breweries/breweries.csv", header=True, inferSchema=True)

    # tablename = "food_category"
    # food_category = spark.read.csv(f"~/opt/bitnami/spark/data/{CSVFOLDER}/{tablename}.csv", header=True, inferSchema=True)
    tablename = "food_nutrient"
    food_nutrient = spark.read.csv(f"data/{CSVFOLDER}/{tablename}.csv", header=True, inferSchema=True)
    tablename = "nutrient_incoming_name"
    nutrient_incoming_name = spark.read.csv(f"data/{CSVFOLDER}/{tablename}.csv", header=True, inferSchema=True)
    tablename = "nutrient"
    nutrient = spark.read.csv(f"data/{CSVFOLDER}/{tablename}.csv", header=True, inferSchema=True)
    tablename = "branded_food"
    branded_food = spark.read.csv(f"data/{CSVFOLDER}/{tablename}.csv", header=True, inferSchema=True)
    tablename = "food"
    food = spark.read.csv(f"data/{CSVFOLDER}/{tablename}.csv", header=True, inferSchema=True)













    spark.stop()