# Databricks notebook source
# MAGIC %md
# MAGIC ###データ準備

# COMMAND ----------

from pyspark.sql.functions import *

csvpath = "/FileStore/sample_data.csv"

sample_df = (spark.read
  .option("header", True)
  .option("inferSchema", True)
  .csv(csvpath))

sample_df = sample_df.withColumn("category", when(col("item_name") == "watermelon", "vegetable").otherwise(col("category")))

display(sample_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE sql_training;

# COMMAND ----------

# MAGIC %md
# MAGIC #Pyspark編

# COMMAND ----------

# MAGIC %md
# MAGIC ###問題1：解答例

# COMMAND ----------

# 解答例1
output_df1 = sample_df.filter(col("category") == "fluit")
output_df2 = output_df1.sort(col("price").asc())
display(output_df2)

# COMMAND ----------

# 解答例2
output_df = sample_df.filter(col("category") == "fluit") \
                     .sort(col("price").asc())
display(output_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###問題2：解答例

# COMMAND ----------

# 解答例1
output_df1 = sample_df.withColumn("price_down", col("price") * 0.9)
output_df2 = output_df1.select("item_name", "category", "price_down")
display(output_df2)

# 3行目についてはselect関数ではなく、drop関数でも記述する事が出来ます。
# output_df2 = output_df1.drop("price", "unit_sales")

# COMMAND ----------

# 解答例2
output_df = sample_df.withColumn("price_down", col("price") * 0.9) \
                     .select("item_name", "category", "price_down")
display(output_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###問題3：解答例

# COMMAND ----------

# 解答例1
output_df1 = sample_df.filter(col("unit_sales") > 10)
output_df2 = output_df1.sort(col("unit_sales").desc())
output_df3 = output_df2.limit(1)
display(output_df3)

# COMMAND ----------

# 解答例2
output_df = sample_df.filter(col("unit_sales") > 10) \
                     .sort(col("unit_sales").desc()) \
                     .limit(1)
display(output_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #SQL編

# COMMAND ----------

# MAGIC %md
# MAGIC ###問題1：解答例

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     item_name,
# MAGIC     unit_sales
# MAGIC FROM
# MAGIC     sample_data
# MAGIC WHERE
# MAGIC     unit_sales < 15
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ###問題2：解答例

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     *
# MAGIC FROM 
# MAGIC     sample_data
# MAGIC ORDER BY
# MAGIC     price ASC
# MAGIC LIMIT 2
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ###問題3：解答例

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     item_name,
# MAGIC     category,
# MAGIC     price_down
# MAGIC FROM
# MAGIC     (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         price * 0.9 AS price_down
# MAGIC     FROM
# MAGIC         sample_data
# MAGIC     )
# MAGIC WHERE
# MAGIC     price_down < 120
# MAGIC ;
