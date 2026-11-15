# nb_02_silver_to_gold
# Microsoft Fabric notebook (PySpark): silver Delta table -> gold star schema
# Data: invented Dubai real estate transactions. Run cells top to bottom.

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver = spark.read.table("silver_transactions")
print("silver rows:", silver.count())        # 97680

# Dimension 1: area (surrogate key = simple number instead of text)
dim_area = (silver.select("area_name_en").distinct()
    .withColumn("area_key", F.row_number().over(Window.orderBy("area_name_en")))
    .select("area_key", "area_name_en"))
dim_area.write.mode("overwrite").format("delta").saveAsTable("gold_dim_area")   # 10 rows

# Dimension 2: calendar covering full years (needed for time intelligence)
b = silver.agg(F.min("instance_date").alias("d0"), F.max("instance_date").alias("d1")).first()
start = f"{b.d0.year}-01-01"
end = f"{b.d1.year}-12-31"
dim_date = (spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date")
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("year_month", F.date_format("date", "yyyy-MM"))
    .withColumn("day_of_week", F.date_format("date", "EEEE")))
dim_date.write.mode("overwrite").format("delta").saveAsTable("gold_dim_date")   # 1461 rows

# Dimension 3: property type and sub type
dim_property = (silver.select("property_type_en", "property_sub_type_en").distinct()
    .withColumn("property_type_key",
                F.row_number().over(Window.orderBy("property_type_en", "property_sub_type_en")))
    .select("property_type_key", "property_type_en", "property_sub_type_en"))
dim_property.write.mode("overwrite").format("delta").saveAsTable("gold_dim_property_type")   # 4 rows

# Fact table: keys + amounts. Left joins keep every transaction.
fact = (silver
    .join(dim_area, "area_name_en", "left")
    .join(dim_property, ["property_type_en", "property_sub_type_en"], "left")
    .withColumn("date_key", F.date_format("instance_date", "yyyyMMdd").cast("int"))
    .select("transaction_id", "date_key", "area_key", "property_type_key",
            "trans_group_en", "reg_type_en", "rooms_en", "has_parking",
            "procedure_area", "actual_worth", "meter_sale_price"))

# Checks: no rows lost or duplicated by the joins, no missing keys
print("fact rows:", fact.count())            # 97680
print("missing keys:", fact.filter(F.col("area_key").isNull()
                                   | F.col("property_type_key").isNull()
                                   | F.col("date_key").isNull()).count())   # 0
fact.write.mode("overwrite").format("delta").saveAsTable("gold_fact_transactions")
