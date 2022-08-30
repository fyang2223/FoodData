import sys
import spark_job_functions as sjf
from pyspark.sql import SparkSession

AIRHOME = sys.argv[1]
CSVFOLDER = sys.argv[2]

if __name__ == '__main__':
    spark = SparkSession.builder.master("spark://spark:7077").appName("NutrientAnalysis").getOrCreate()

    food_category = sjf.csv_to_df(f"hdfs://namenode:9000/data/FoodData/{CSVFOLDER}/food_category.csv", spark)
    food_nutrient = sjf.csv_to_df(f"hdfs://namenode:9000/data/FoodData/{CSVFOLDER}/food_nutrient.csv", spark)
    nutrient_incoming_name = sjf.csv_to_df(f"hdfs://namenode:9000/data/FoodData/{CSVFOLDER}/nutrient_incoming_name.csv", spark)
    nutrient = sjf.csv_to_df(f"hdfs://namenode:9000/data/FoodData/{CSVFOLDER}/nutrient.csv", spark)
    branded_food = sjf.csv_to_df(f"hdfs://namenode:9000/data/FoodData/{CSVFOLDER}/branded_food.csv", spark)
    food = sjf.csv_to_df(f"hdfs://namenode:9000/data/FoodData/{CSVFOLDER}/food.csv", spark)

    nutrient_amounts = sjf.food_nutrient_join_nutrient(food_nutrient, nutrient)
    nutrient_amounts = sjf.filter_invalid_nutrient_amounts(nutrient_amounts)

    food = sjf.food_tables_id_to_int(food)
    branded_food = sjf.food_tables_id_to_int(branded_food)

    food_combined = sjf.food_join_branded(food, branded_food)

    nutrient_all = sjf.all_nutrient_join_all_food(nutrient_amounts, food_combined)

    res = sjf.summarize(nutrient_all)

    res = res.orderBy("name", "avg(amount)", ascending=False) \
        .withColumnRenamed("count(1)", "sample_size") \
        .withColumnRenamed("avg(amount)", "avg(amount) per 100g")

    #Add the units to amount
    res = res.join(nutrient, ["name"]) \
        .drop("id", "nutrient_nbr", "rank")

    pandasres = res.toPandas()

    pandasres.to_csv(f"{AIRHOME}/output.csv")


    spark.stop()