# Databricks notebook source
# MAGIC %md
# MAGIC #PySpark編

# COMMAND ----------

# MAGIC %md
# MAGIC 練習問題では、先程の研修で扱ったデータフレームを引き続き使用します。<br>
# MAGIC (スイカの商品区分のみ修正を加えています。)

# COMMAND ----------

# 学習用データフレームの読み込み
# %run /Shared/new_employee_training/databricks-training/Setup

# 上記%runコマンドが急に動かなくなったため、直書きでコマンドを一旦記述
# 本来はSetup2のようなファイルを作成し、それを実行する予定
from pyspark.sql.functions import *

csvpath = "/FileStore/sample_data.csv"

sample_df = (spark.read
  .option("header", True)
  .option("inferSchema", True)
  .csv(csvpath))

sample_df = sample_df.withColumn("category", when(col("item_name") == "watermelon", "vegetable").otherwise(col("category")))

display(sample_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###問題1
# MAGIC 以下の操作を行い、下図のようなデータを持つデータフレームを作成してください。<br>
# MAGIC ・商品区分(category)が果物のデータのみを抽出<br>
# MAGIC ・販売価格(price)が低い順番にデータが並び替え<br>
# MAGIC 
# MAGIC 
# MAGIC |  item_name  |  category  |  price  | unit_sales |
# MAGIC | ---- | ---- | ---- | ---- |
# MAGIC |  banana  |  fluit  |  90  |  15  |
# MAGIC |  apple  |  fluit  |  100  |  20  |
# MAGIC |  peach  |  fluit  |  130  |  10  |

# COMMAND ----------

# TODO


# COMMAND ----------

# MAGIC %md
# MAGIC ###問題2
# MAGIC 以下の操作を行い、下図のようなデータを持つデータフレームを作成してください。<br>
# MAGIC ・販売価格(price)に0.9を掛けた値を新しいカラム「price_down」として作成<br>
# MAGIC ・商品名(item_name)、商品区分(category)、「price_down」の3つのカラムのみを保持<br>
# MAGIC 
# MAGIC 
# MAGIC |  item_name  |  category  |  price_down  |
# MAGIC | ---- | ---- | ---- |
# MAGIC |  apple  |  fluit  |  90  |
# MAGIC |  banana  |  fluit  |  81  |
# MAGIC |  peach  |  fluit  |  117  |
# MAGIC |  watermelon  |  vegetable  |  270  |
# MAGIC |  onion  |  vegetable  |  135  |
# MAGIC |  tomato  |  vegetable  |  108  |

# COMMAND ----------

# TODO


# COMMAND ----------

# MAGIC %md
# MAGIC ###問題3
# MAGIC 以下の操作を行い、下図のようなデータを持つデータフレームを作成してください。<br>
# MAGIC ・販売個数(unit_sales)が10よりも多いデータのみを抽出<br>
# MAGIC ・販売個数(unit_salees)が多い順番に並び替え<br>
# MAGIC ・先頭から1行目のデータのみを保持
# MAGIC 
# MAGIC 
# MAGIC |  item_name  |  category  |  price  | unit_sales |
# MAGIC | ---- | ---- | ---- | ---- |
# MAGIC |  apple  |  fluit  |  100  |  20  |

# COMMAND ----------

# TODO

