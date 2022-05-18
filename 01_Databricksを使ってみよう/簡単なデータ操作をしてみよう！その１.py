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
csvpath = "/FileStore/sample_data.csv"

print("データフレーム名：sample_df")

sample_df = (spark.read
  .option("header", True)
  .option("inferSchema", True)
  .csv(csvpath))

display(sample_df)

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
# MAGIC |  カラム名  |  説明  |
# MAGIC | ---- | ---- |
# MAGIC |  item_name  |  商品名  |
# MAGIC |  category  |  商品区分(果物 or 野菜)  |
# MAGIC |  price  |  販売価格  |
# MAGIC |  unit_sales  |  販売個数  |

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
# MAGIC 関数の実行結果は、「output_df」という名前のデータフレームに出力するようにします。<br><br>
# MAGIC 今回はsample_dfを入力データフレームとしてselect関数を適用し、<br>
# MAGIC item_name(商品名)とprice(販売価格)のデータのみを取得します。

# COMMAND ----------

# 出力データフレームの名前は「output_df」とします。
output_df = sample_df.select("item_name", "price")

# COMMAND ----------

# MAGIC %md
# MAGIC データフレームの中身を確認する際にはdisplayコマンドを使用します。<br><br>
# MAGIC 構文<br>
# MAGIC ```display(出力データフレーム)```<br><br>
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
# MAGIC withColumn関数を使用する事で、新しいカラムを追加する事が出来ます。<br>
# MAGIC (Columnの**C**は大文字であることに注意！)<br><br>
# MAGIC 構文<br>
# MAGIC ```出力データフレーム名 = 入力データフレーム名.withColumn("新しいカラム名", 値)```<br><br>
# MAGIC 例<br>
# MAGIC ```output_df = sample_df.withColumn("price_up", col("price") + 10)```<br>
# MAGIC ※**カラムの値を関数内で使用するためには、カラム名をcol()関数で囲む必要があります。**<br><br>
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
# MAGIC ###・先頭n行のデータの表示

# COMMAND ----------

# MAGIC %md
# MAGIC limit()関数を使用する事で、先頭から指定した行数分だけデータを取得する事が出来ます。<br><br>
# MAGIC 構文<br>
# MAGIC ```出力データフレーム = 入力データフレーム.limit(n)```<br>
# MAGIC ※nには取り出したい行数を入力します。<br><br>
# MAGIC 例<br>
# MAGIC ```output_df = sample_df.limit(3)```<br>
# MAGIC (sample_dfの**先頭3行**をoutput_dfに出力しています。)

# COMMAND ----------

# MAGIC %md
# MAGIC 実際に上記のコード例を実行してみましょう。

# COMMAND ----------

output_df = sample_df.limit(3)
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
# MAGIC ###パターン1：withColumn + select(drop)

# COMMAND ----------

# MAGIC %md
# MAGIC 商品価格(price)と販売個数(unit_sales)を掛けた値を新しいカラム「total_sales」として作成し、(withColumnのコード例と同じ)<br>
# MAGIC その後、商品名(item_name)とtotal_salesのカラムのみを取得してデータフレームに出力します。(select/dropのどちらかで実行)<br>
# MAGIC 
# MAGIC ※最終出力のデータフレームのイメージ
# MAGIC 
# MAGIC |  item_name  |  total_sales  |
# MAGIC | ---- | ---- |
# MAGIC |  apple  |  2000  |
# MAGIC |  banana  |  1350  |
# MAGIC |  peach  |  1300  |
# MAGIC |  watermelon  |  1500  |
# MAGIC |  onion  |  3000  |
# MAGIC |  tomato  |  1200  |

# COMMAND ----------

# ここまでは先程のコード例と同じ(便宜上、出力データフレーム名は「output_df1」としています。)
output_df1 = sample_df.withColumn("total_sales", col("price") * col("unit_sales"))
display(output_df1)

# COMMAND ----------

# 上の出力データフレーム「output_df1」を今度は入力で使用し、「output_df2」にselect関数の実行結果を出力します。
output_df2 = output_df1.select("item_name", "total_sales")
display(output_df2)

# COMMAND ----------

# MAGIC %md
# MAGIC 今回、withColumnとselectの処理を2つに分けて記述しましたが、<br>
# MAGIC 下記のように2つの処理を一つにまとめて記述する事も可能です。<br><br>
# MAGIC ※注意点<br>
# MAGIC ・関数の後ろにもう一つの関数を繋げる形で記述します。
# MAGIC ```
# MAGIC sample_df.withColumn(~~~~~~) \
# MAGIC          .select(~~~~~~)
# MAGIC ```
# MAGIC 
# MAGIC ・複数行にわたって1つのデータフレームに関数を使用する場合、**改行**(\\)の記号が必要になります。<br>
# MAGIC (上の例では1行目の一番後ろの部分)

# COMMAND ----------

output_df = sample_df.withColumn("total_sales", col("price") * col("unit_sales")) \
                     .select("item_name", "total_sales")

display(output_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###パターン2：sort + limit

# COMMAND ----------

# MAGIC %md
# MAGIC sample_dfを商品価格(price)の高い順に並び替えた後、<br>
# MAGIC 先頭1行のみをデータフレームに出力します。<br><br>
# MAGIC 最終出力のイメージ
# MAGIC |  item_name  |  category  |  price  | unit_sales |
# MAGIC | ---- | ---- | ---- | ---- |
# MAGIC |  watermelon  |  fluit  |  300  |  5  |

# COMMAND ----------

# MAGIC %md
# MAGIC ・**2つに分けて記述する場合**

# COMMAND ----------

output_df1 = sample_df.sort(col("price").desc())
display(output_df1)

# COMMAND ----------

output_df2 = output_df1.limit(1)
display(output_df2)

# COMMAND ----------

# MAGIC %md
# MAGIC **1つにまとめて記述する場合**

# COMMAND ----------

output_df = sample_df.sort(col("price").desc()) \
                     .limit(1)

display(output_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###応用：withColumn + when

# COMMAND ----------

# MAGIC %md
# MAGIC ※発展的な内容になるため、今の時点でコードを理解する必要はありません。参考程度に見てみてください。<br><br>
# MAGIC 先程の実行結果をよく見ると、スイカ(watermelon)の商品区分が果物(fluit)になっています。<br>
# MAGIC スイカは野菜であるため、スイカの商品区分を野菜(vegetable)に変更していきます。

# COMMAND ----------

# MAGIC %md
# MAGIC when関数を使用する事で、指定した条件に合致するデータを別の値に置き替える事が出来ます。<br>
# MAGIC (SQLでいう**UPDATE文**と同等の処理)<br><br>
# MAGIC 構文<br>
# MAGIC ```出力データフレーム = 入力データフレーム.withColumn("置き替えたい値があるカラム名", when(条件式, 条件に一致した場合に置き替える値).otherwise(条件に一致しない場合の値))```<br><br>
# MAGIC 例<br>
# MAGIC ```output_df = sample_df.withColumn("category", when(col("item_name") == "watermelon", "vegetable").otherwise(col("category")))```<br><br>
# MAGIC 今回の例では、<br>
# MAGIC 
# MAGIC **置き替えたい値があるカラム名**　→　商品区分(category)<br><br>
# MAGIC 商品名がスイカのデータを書き換えたいので、<br>
# MAGIC **条件式**　→　col("item_name") == "watermelon"<br><br>
# MAGIC 果物から野菜にデータを置き替えたいので、<br>
# MAGIC **条件に一致した場合に置き替える値** →　"vegetable"<br><br>
# MAGIC スイカ以外のデータは元々の商品区分のままにしておくため、<br>
# MAGIC **条件に一致しない場合の値**　→　col("category")<br><br>
# MAGIC となります。<br><br>
# MAGIC ※withColumn関数は、新しいカラムの追加だけでなく、<br>
# MAGIC 既存のカラムを別の値で置き替える際にも使用する事が出来ます。

# COMMAND ----------

# MAGIC %md
# MAGIC 実際に、上記のコード例を動かしてみましょう。

# COMMAND ----------

output_df = sample_df.withColumn("category", when(col("item_name") == "watermelon", "vegetable").otherwise(col("category")))
display(output_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 以上で「簡単なデータ操作をしてみよう！その１」は終了となります。<br>
# MAGIC 続いて「簡単なデータ操作をしてみよう！その２」に移ります。
