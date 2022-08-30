from pyspark.sql.functions import col
from pyspark.sql.functions import when
from pyspark.sql.functions import count, lit, avg

def csv_to_df(filename, SS):
    try:
        df = SS.read.csv(filename, header=True, inferSchema=True)
        return df
    except:
        print("Error with reading CSV file to Spark Dataframe")

# Fn called by MAIN
def food_nutrient_join_nutrient(food_nutrient, nutrient):
    nutrient_amounts = food_nutrient.join(nutrient, food_nutrient["nutrient_id"] == nutrient["nutrient_nbr"])
    nutrient_amounts = nutrient_amounts.select(col("fdc_id"), col("name"), col("amount"), col("unit_name"))
    
    return nutrient_amounts

# Fn called by MAIN
# TESTED
def filter_invalid_nutrient_amounts(nutrient_amounts): 
    nutrient_amounts = nutrient_amounts.withColumn("keep_row", 
            when((nutrient_amounts.amount >= 100) & (nutrient_amounts.unit_name == "G"), False)
            .when((nutrient_amounts.amount >= 100000) & (nutrient_amounts.unit_name == "MG"), False)
            .otherwise(True)) \
        .filter(col("keep_row") == True) \
        .drop("keep_row") \
        .orderBy("amount", ascending=False)
    
    return nutrient_amounts

# Fn called by MAIN
# TESTED
def food_tables_id_to_int(food):
    #Filter out rows where fdc_id is not an integer. When casting a string to int, pyspark will set non-integers to null.
    food = food.filter(col("fdc_id").cast("int").isNotNull())

    #Then cast fdc_id to integers. Branded_food has no weird values, so just convert to int.
    food = food.withColumn("fdc_id", food["fdc_id"].cast("Integer"))
    
    return food

# Fn called by MAIN
def food_join_branded(food, branded_food):
    #Outer Join or Right Join the two large food tables as "food" has some fdc_ids that are not in "branded_food".
    food_combined = branded_food.join(food, branded_food["fdc_id"] == food["fdc_id"], how="outer") \
        .select(food["fdc_id"], #Note the use of food["fdc_id"], not branded_food["fdc_id"]
                branded_food["brand_owner"], 
                branded_food["branded_food_category"],
                food["description"],
                food["data_type"],
                food["food_category_id"])
    
    return food_combined

# Fn called by MAIN
def all_nutrient_join_all_food(nutrient_amounts, food_combined):
    nutrient_all = nutrient_amounts.join(food_combined, nutrient_amounts["fdc_id"] == food_combined["fdc_id"])
    nutrient_all = nutrient_all.select("name", "amount", "branded_food_category")
    return nutrient_all

# Fn called by MAIN
# TESTED
def summarize(nutrient_all):
    #For each unique (vitamin, food_category) pair show the average vitamin amount. Also show how many unique products in each as count.
    res = nutrient_all \
        .groupby("name", "branded_food_category") \
        .agg(count(lit(1)),
             avg(nutrient_all["amount"]))
    return res












