# Databricks notebook source
# MAGIC %md
# MAGIC ## AutoMLで学習したモデルを用いて予測をしてみよう

# COMMAND ----------

# MAGIC %md
# MAGIC ## 環境設定
# MAGIC 今回利用したいデータベースを指定します。

# COMMAND ----------

import re
course = "automl_demo"
username = spark.sql("SELECT current_user()").first()[0]
database = f"{re.sub('[^a-zA-Z0-9]', '_', username)}_{course}"

spark.sql(f"USE {database}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 入力データの読み込み
# MAGIC 各自学習に利用したデータのみコメントアウトを外し、それ以外についてはコメントアウトのままにして下さい。<br>
# MAGIC ※本来は予測は学習に使っていないデータでするものですが、今回は簡単のためそのまま全入力データを使っています。

# COMMAND ----------

import pandas as pd

inputds = spark.read.table("titanic_training").toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ### モデルの読み込み
# MAGIC AutoMLで学習したモデルを読み込みます。<br>
# MAGIC model_uri = *** については各自下記手順で取得した値を設定して下さい。
# MAGIC
# MAGIC 1. 実行したエクスペリメント上の「最適なモデルのノートブックを表示」をクリック
# MAGIC 1. 画像を参考に「/runs/***/artifactPath/model」をコピーし、下のセルに貼り付ける
# MAGIC
# MAGIC 最適なモデル以外のモデルについても、下の表の「Source」列の「Notebook」を押すことで同様にモデルを取得できます。
# MAGIC
# MAGIC <img src="/files/automl_demo/Model_Select.png">

# COMMAND ----------

import mlflow
model_uri = "runs:/******/model"
model = mlflow.pyfunc.load_model(model_uri)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 予測の実行

# COMMAND ----------

pred = model.predict(inputds)
pred

# COMMAND ----------

# MAGIC %md
# MAGIC ### 混同行列の表示
# MAGIC target = *** については予測した目的変数名以外はコメントアウトして下さい。

# COMMAND ----------

target = "Survived"

from sklearn.metrics import confusion_matrix

input_target = inputds[[target]]
print(confusion_matrix(input_target, pred))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 適合率(Precision)、再現率(Recall)、F1スコア(F1score)

# COMMAND ----------

from sklearn.metrics import classification_report
print(classification_report(input_target, pred))
