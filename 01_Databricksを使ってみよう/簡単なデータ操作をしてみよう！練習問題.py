# Databricks notebook source
# MAGIC %md
# MAGIC #PySpark編

# COMMAND ----------

# MAGIC %md
# MAGIC 練習問題では、先程の研修で扱ったデータフレームを引き続き使用します。<br>
# MAGIC (スイカの商品区分のみ修正を加えています。)

# COMMAND ----------

# 学習用データフレームの読み込み
from pyspark.sql.functions import *

csvpath = "/FileStore/sample_data.csv"

sample_df = (spark.read
  .option("header", True)
  .option("inferSchema", True)
  .csv(csvpath))

# スイカの商品区分を修正
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


# COMMAND ----------

# MAGIC %md
# MAGIC #SQL編

# COMMAND ----------

# MAGIC %md
# MAGIC 先程と同じデータベース、テーブルを使用していきます。
# MAGIC 
# MAGIC データベース：**sql_training**<br>
# MAGIC テーブル：**sample_data**<br>
# MAGIC 
# MAGIC |  item_name  |  category  |  price  | unit_sales |
# MAGIC | ---- | ---- | ---- | ---- |
# MAGIC |  apple  |  fluit  |  100  |  20  |
# MAGIC |  banana  |  fluit  |  90  |  15  |
# MAGIC |  peach  |  fluit  |  130  |  10  |
# MAGIC |  watermelon  |  fluit  |  300  |  5  |
# MAGIC |  onion  |  vegetable  |  150  |  20  |
# MAGIC |  tomato  |  vegetable  |  120  |  10  |

# COMMAND ----------

# MAGIC %sql
# MAGIC USE sql_training;

# COMMAND ----------

# MAGIC %md
# MAGIC ###問題1
# MAGIC 以下の操作を行い、下図のようなデータを持つテーブルを作成してください。<br>
# MAGIC ・販売個数(unit_sales)が15よりも小さいデータのみを抽出<br>
# MAGIC ・商品名(item_name)、販売個数(unit_sales)の2つのカラムのみを保持<br>
# MAGIC |  item_name  |  unit_sales |
# MAGIC | ---- | ---- |
# MAGIC |  peach |  10  |
# MAGIC |  watermelon |  5  |
# MAGIC |  tomato |  10  |

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ###問題2
# MAGIC 以下の操作を行い、下図のようなデータを持つテーブルを作成してください。<br>
# MAGIC ・販売価格(price)が低い順に並び替え<br>
# MAGIC ・先頭2行の全てのカラムを保持
# MAGIC |  item_name  |  category  |  price  | unit_sales |
# MAGIC | ---- | ---- | ---- | ---- |
# MAGIC |  banana  |  fluit  |  90  |  15  |
# MAGIC |  apple  |  fluit  |  100  |  20  |

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ###問題3(発展)
# MAGIC 以下の操作を行い、下図のようなデータを持つテーブルを作成してください。<br>
# MAGIC ・販売価格(price)に0.9を掛けた値を新しいカラム「price_down」として作成<br>
# MAGIC ・「price_down」のカラムの値が120よりも小さいデータのみを抽出<br>
# MAGIC ・商品名(item_name)、商品区分(category)、「price_down」の3つのカラムのみを保持<br><br>
# MAGIC ※サブクエリの記述が必要となります。
# MAGIC |  item_name  |  category  |  price_down  |
# MAGIC | ---- | ---- | ---- |
# MAGIC |  apple  |  fluit  |  90.0  |
# MAGIC |  banana  |  fluit  |  81.0  |
# MAGIC |  peach  |  fluit  |  117.0  |
# MAGIC |  tomato  |  vegetable  |  108.0  |

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- TODO
