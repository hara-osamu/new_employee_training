# Databricks notebook source
# MAGIC %md
# MAGIC ###練習1 解答
# MAGIC 
# MAGIC ```
# MAGIC SELECT
# MAGIC     trip_distance,
# MAGIC     fare_amount
# MAGIC FROM
# MAGIC     samples.nyctaxi.trips
# MAGIC ORDER BY 
# MAGIC     trip_distance DESC
# MAGIC LIMIT 
# MAGIC     10
# MAGIC ;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ###練習2 解答
# MAGIC ```
# MAGIC SELECT
# MAGIC     *
# MAGIC FROM 
# MAGIC     samples.nyctaxi.trips
# MAGIC WHERE
# MAGIC     pickup_zip = "10110" AND 
# MAGIC     dropoff_zip = "10282"
# MAGIC ;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ###練習3 解答
# MAGIC ```
# MAGIC SELECT
# MAGIC     tpep_pickup_date,
# MAGIC     tpep_dropoff_date,
# MAGIC     trip_distance,
# MAGIC     fare_amount
# MAGIC FROM
# MAGIC     (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         DATE(tpep_pickup_datetime) AS tpep_pickup_date,
# MAGIC         DATE(tpep_dropoff_datetime) AS tpep_dropoff_date
# MAGIC     FROM
# MAGIC         samples.nyctaxi.trips
# MAGIC     )
# MAGIC WHERE
# MAGIC     tpep_pickup_date = "2016-02-14"
# MAGIC ;
# MAGIC ```
