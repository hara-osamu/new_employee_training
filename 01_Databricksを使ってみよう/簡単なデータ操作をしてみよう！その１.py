# Databricks notebook source
# MAGIC %md
# MAGIC #Pysparkを利用したデータ操作

# COMMAND ----------

# MAGIC %md
# MAGIC これからPySparkというプログラミング言語を扱っていきます。<br><br>
# MAGIC **-PySparkとは-**<br>
# MAGIC PySparkは、**Apache Spark**というフレームワークが元になっています。<br><br>
# MAGIC Apache Sparkは、ビッグデータを高速で分散処理を行うことが可能であり、<br>
# MAGIC Scalaというプログラミング言語で記述されています。<br>
# MAGIC 
# MAGIC このApache SparkをPythonで実行できるように作られたのが、PySparkとなっています。

# COMMAND ----------

# MAGIC %md
# MAGIC まずは研修で使用するサンプルデータを作成します。下のコードを実行してみましょう。<br>
# MAGIC コード実行の仕方・・・実行したいセルをクリックして、右側に表示されている▶をクリック or Ctrl+Enter

# COMMAND ----------

# 学習用データフレームの読み込み
# %run /Shared/new_employee_training/databricks-training/Setup

# 上記%runコマンドが急に動かなくなったため、直書きでコマンドを一旦記述
csvpath = "/FileStore/sample_data.csv"

sample_df = (spark.read
  .option("header", True)
  .option("inferSchema", True)
  .csv(csvpath))

# COMMAND ----------

# MAGIC %md
# MAGIC ##データフレームとカラムについて

# COMMAND ----------

# MAGIC %md
# MAGIC データをテーブルの形式で定義したものを**データフレーム**と呼びます。<br>
# MAGIC データフレームは複数の行と列で構成された2次元配列となっています。<br>
# MAGIC <br>
# MAGIC 列の名前(item_name, category...)を**カラム**と呼びます。<br>
# MAGIC 各カラムの説明は以下の通りです。<br>
# MAGIC ・item_name・・・商品名<br>
# MAGIC ・category・・・商品区分(果物 or 野菜)<br>
# MAGIC ・price・・・販売価格<br>
# MAGIC ・unit_sales・・・販売個数

# COMMAND ----------

# MAGIC %md
# MAGIC それでは、実際にPySparkの関数を利用して、<br>
# MAGIC データの操作を行っていきましょう。<br><br>
# MAGIC まずは、使用する関数のインポートから進めていきます。

# COMMAND ----------

#データ操作で使用する関数のインポート
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###・データの取得

# COMMAND ----------

# MAGIC %md
# MAGIC select関数を使用する事で、特定のカラムのデータのみを取得する事が出来ます。<br><br>
# MAGIC 構文<br>
# MAGIC ```出力データフレーム = 入力データフレーム.select("カラム名",,,)```<br><br>
# MAGIC 例<br>
# MAGIC ```output_df = sample_df.select("item_name", "category")```

# COMMAND ----------

# MAGIC %md
# MAGIC 今回はsample_dfを入力データフレームとしてselect関数を適用し、<br>
# MAGIC item_name(商品名)とprice(販売価格)のデータのみを取得します。<br><br>
# MAGIC 関数の実行結果は、「output_df」という名前のデータフレームに出力するようにします。

# COMMAND ----------

# 出力データフレームの名前は「output_df」とします。
output_df = sample_df.select("item_name", "price")

# COMMAND ----------

# MAGIC %md
# MAGIC データフレームの中身を確認する際にはdisplayコマンドを使用します。<br><br>
# MAGIC 構文<br>
# MAGIC ```display(データフレーム)```<br><br>
# MAGIC 先程の出力データフレーム(output_df)が正しく処理されているか確認してみましょう。

# COMMAND ----------

display(output_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###・データの削除

# COMMAND ----------

# MAGIC %md
# MAGIC drop関数を使用する事で、特定のカラムのデータを削除する事が出来ます。<br><br>
# MAGIC 構文<br>
# MAGIC ```出力データフレーム = 入力データフレーム.drop("カラム名",,,)```<br><br>
# MAGIC 例<br>
# MAGIC ```output_df = sample_df.drop("price", "unit_sales")```

# COMMAND ----------

# MAGIC %md
# MAGIC 先程とは反対に、sample_dfのitem_name(商品名)とprice(販売価格)のカラムを削除してみます。<br>
# MAGIC 実行の結果はoutput_dfに出力します。

# COMMAND ----------

output_df = sample_df.drop("item_name", "price")
display(output_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###・カラムの追加

# COMMAND ----------

# MAGIC %md
# MAGIC withColumn関数を使用する事で、新しいカラムを追加する事が出来ます。<br><br>
# MAGIC 構文<br>
# MAGIC ```出力データフレーム名 = 入力データフレーム名.withColumn("新しいカラム名", 値)```<br><br>
# MAGIC 例<br>
# MAGIC ```output_df = sample_df.withColumn("price_up", col("price") + 10)```<br>
# MAGIC ※カラムの値を計算式で使用するためには、カラム名をcol()関数で囲む必要があります。<br><br>
# MAGIC 例では商品価格(price)を10円値上げした値を、「price_up」という新しいカラムに入れる処理を行っています。

# COMMAND ----------

# MAGIC %md
# MAGIC 今回はprice(販売価格)とunit_sales(販売個数)の値を掛け合わせた結果(売上金額)を、<br>
# MAGIC **total_sales**という新しいカラムに追加する処理を実行してみます。

# COMMAND ----------

output_df = sample_df.withColumn("total_sales", col("price") * col("unit_sales"))
display(output_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###・データの抽出

# COMMAND ----------

# MAGIC %md
# MAGIC filter関数を使用する事で、特定の条件に合致したデータのみを抽出する事が出来ます。<br><br>
# MAGIC 構文<br>
# MAGIC ```出力データフレーム名 = 入力データフレーム名.filter(条件式)```<br><br>
# MAGIC 例<br>
# MAGIC ```output_df = sample_df.filter(col("item_name") == "apple")```<br><br>
# MAGIC 例では、商品名(item_name)が「apple」のデータのみを抽出しています。

# COMMAND ----------

# MAGIC %md
# MAGIC 今回は商品価格が100円よりも高い商品のデータのみを抽出します。

# COMMAND ----------

output_df = sample_df.filter(col("price") > 100)
display(output_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###・データのソート

# COMMAND ----------

# MAGIC %md
# MAGIC sort関数を使用する事でデータの表示順を操作する事が出来ます。<br><br>
# MAGIC ・構文(他にも記述の仕方はあり、あくまで一例となります。)<br><br>
# MAGIC ```出力データフレーム名 = 入力データフレーム名.sort(col("カラム名").option)```<br>
# MAGIC ※optionの場所には並び替えの順序を指定する事が出来ます。<br>
# MAGIC 昇順での並び替え→.asc()<br>
# MAGIC 降順での並び替え→.desc()<br><br>
# MAGIC ※順序を指定しない場合には**昇順**で並び替えられます。

# COMMAND ----------

# MAGIC %md
# MAGIC 今回は、販売個数(unit_sales)の多い順(降順)にデータを並び替えます。

# COMMAND ----------

output_df = sample_df.sort(col("unit_sales").desc())
display(output_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##複数の関数を組み合わせてみよう！

# COMMAND ----------

# MAGIC %md
# MAGIC 今まで学習した関数を組み合わせて実行する事で、<br>
# MAGIC より複雑な処理を行うことができるようになります。

# COMMAND ----------

# MAGIC %md
# MAGIC ###パターン1：withColuumn + select<br><br>

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC ###参考

# COMMAND ----------

# MAGIC %md
# MAGIC データフレームをよく見ると、スイカ(watermelon)の商品区分が果物(fluit)になっています。<br>
# MAGIC スイカは野菜であるため、以下のコードを実行し、スイカの商品区分を野菜(vegetable)に変更します。

# COMMAND ----------

sample_df = sample_df.withColumn("category", when(col("item_name") == "watermelon", "vegetable").otherwise(col("category")))
display(sample_df)
