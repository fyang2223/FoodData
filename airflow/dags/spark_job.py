import sys
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col
from pyspark.sql.functions import when
from pyspark.sql.functions import count, lit, avg

AIRHOME = sys.argv[1]
CSVFOLDER = sys.argv[2]

def allnutrient_join_names(nutrient, food_nutrient):
    #Inner join the vitamin id and amounts above to the larger food_nutrients table
    nutrient_amounts = food_nutrient.join(nutrient, food_nutrient["nutrient_id"] == nutrient["nutrient_nbr"]) \
        .select(col("fdc_id"), col("name"), col("amount"), col("unit_name"))

    #Keep only the rows where the "amount" of vitamin measured per 100g is LESS than 100g.
    #Some have measurement/data errors that show more than 100g of vitamin per 100g.
    nutrient_amounts = nutrient_amounts.withColumn("keep_row", 
            when((nutrient_amounts.amount >= 100) & (nutrient_amounts.unit_name == "G"), False)
            .when((nutrient_amounts.amount >= 100000) & (nutrient_amounts.unit_name == "MG"), False)
            .otherwise(True)) \
        .filter(col("keep_row") == True) \
        .drop("keep_row") \
        .orderBy("amount", ascending=False)
    
    return nutrient_amounts

def food_join_owner_category(food, branded_food):
    #Filter out rows where fdc_id is not an integer. When casting a string to int, pyspark will set non-integers to null.
    food = food.filter(col("fdc_id").cast("int").isNotNull())

    #Then cast fdc_id to integers. Branded_food has no weird values, so just convert to int.
    food = food.withColumn("fdc_id", food["fdc_id"].cast("Integer"))
    branded_food = branded_food.withColumn("fdc_id", branded_food["fdc_id"].cast("Integer"))
    
    #Outer Join or Right Join the two large food tables as "food" has some fdc_ids that are not in "branded_food".
    food_combined = branded_food.join(food, branded_food["fdc_id"] == food["fdc_id"], how="outer") \
        .select(food["fdc_id"], #Note the use of food["fdc_id"], not branded_food["fdc_id"]
                branded_food["brand_owner"], 
                branded_food["branded_food_category"],
                food["description"],
                food["data_type"],
                food["food_category_id"])
    
    return food_combined

def all_nutrient_join_all_food(nutrient_amounts, food_combined):
    nutrient_all = nutrient_amounts.join(food_combined, nutrient_amounts["fdc_id"] == food_combined["fdc_id"])
    nutrient_subset = nutrient_all.select("name", "amount", "branded_food_category")
    return nutrient_subset

def summarize(nutrient_subset):
    #For each unique (vitamin, food_category) pair show the average vitamin amount. Also show how many unique products in each as count.
    res = nutrient_subset \
        .groupby("name", "branded_food_category") \
        .agg(count(lit(1)),
             avg(nutrient_subset["amount"]))
    
    res = res.orderBy("name", "avg(amount)", ascending=False) \
        .withColumnRenamed("count(1)", "sample_size") \
        .withColumnRenamed("avg(amount)", "avg(amount) per 100g")
    
    #Add the units to amount
    res = res.join(nutrient, ["name"]) \
        .drop("id", "nutrient_nbr", "rank")
    return res





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





if __name__ == '__main__':
    spark = SparkSession.builder.master("spark://spark:7077").appName("NutrientAnalysis").getOrCreate()

    tablename = "food_category"
    food_category = spark.read.csv(f"hdfs://namenode:9000/data/FoodData/{CSVFOLDER}/{tablename}.csv", header=True, inferSchema=True)
    tablename = "food_nutrient"
    food_nutrient = spark.read.csv(f"hdfs://namenode:9000/data/FoodData/{CSVFOLDER}/{tablename}.csv", header=True, inferSchema=True)
    tablename = "nutrient_incoming_name"
    nutrient_incoming_name = spark.read.csv(f"hdfs://namenode:9000/data/FoodData/{CSVFOLDER}/{tablename}.csv", header=True, inferSchema=True)
    tablename = "nutrient"
    nutrient = spark.read.csv(f"hdfs://namenode:9000/data/FoodData/{CSVFOLDER}/{tablename}.csv", header=True, inferSchema=True)
    tablename = "branded_food"
    branded_food = spark.read.csv(f"hdfs://namenode:9000/data/FoodData/{CSVFOLDER}/{tablename}.csv", header=True, inferSchema=True)
    tablename = "food"
    food = spark.read.csv(f"hdfs://namenode:9000/data/FoodData/{CSVFOLDER}/{tablename}.csv", header=True, inferSchema=True)

    nutrient_amounts = allnutrient_join_names(nutrient, food_nutrient)

    food_combined = food_join_owner_category(food, branded_food)

    nutrient_subset = all_nutrient_join_all_food(nutrient_amounts, food_combined)

    res = summarize(nutrient_subset)

    pandasres = res.toPandas()

    pandasres.to_csv(f"{AIRHOME}/output.csv")





    spark.stop()