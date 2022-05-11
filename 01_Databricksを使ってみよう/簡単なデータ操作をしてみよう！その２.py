# Databricks notebook source
# MAGIC %md
# MAGIC #SQLを利用したデータ操作

# COMMAND ----------

# MAGIC %md
# MAGIC Databricksの環境では、同じノートブック内で複数の言語を扱うことが出来ます。<br>
# MAGIC 今回は**SQL**を利用して上と同じデータ操作を行ってみましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC ###マジックコマンドについて

# COMMAND ----------

# MAGIC %md
# MAGIC このノートブックのデフォルトの言語はPythonとなっています。<br>
# MAGIC デフォルトの言語 → 画面上部のノートブック名の右部の丸内に記載されています。<br><br>
# MAGIC 
# MAGIC デフォルトの言語と異なるSQLを使用するためには、マジックコマンド(%sql)をコードの先頭におきます。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Let's SQL Practice!"

# COMMAND ----------

# MAGIC %md
# MAGIC ###データベースとテーブルについて

# COMMAND ----------

# MAGIC %md
# MAGIC それぞれの用語の定義については以下の通りです。<br><br>
# MAGIC ・**データベース** → テーブルの集合体<br>
# MAGIC ・**テーブル**　　 → 構造化されたデータの集合体<br>
# MAGIC ※構造化・・・行と列の概念を持ち、項目のデータ型や区切り文字等が事前に設定されていること<br><br>
# MAGIC 
# MAGIC 大まかに、データベースの中にテーブルが存在しているんだという認識で大丈夫です。

# COMMAND ----------

# MAGIC %md
# MAGIC 今回の研修では、以下の名前のデータベースと、その中にあるテーブルを使用します。<br><br>
# MAGIC データベース：**sql_training**<br>
# MAGIC テーブル：**sample_data**<br><br>
# MAGIC テーブル内のデータは、先程のデータフレームの中身と全く同じものになっています。

# COMMAND ----------

# MAGIC %md
# MAGIC まずは使用するデータベースを指定して、データベース内のテーブルをプログラムで使用できるようにします。<br>
# MAGIC 使用コマンド：**USE + データベース名;**<br>
# MAGIC (※文末のセミコロンは、ここまででコマンドが終了しますよという合図のようなものです。)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE sql_training;

# COMMAND ----------

# MAGIC %md
# MAGIC ###・データの取得

# COMMAND ----------

# MAGIC %md
# MAGIC まずはSELECT文でテーブルの中のデータを確認してみます。<br>
# MAGIC ・構文<br>
# MAGIC ```
# MAGIC SELECT ~~~ 
# MAGIC FROM ~~~
# MAGIC ;
# MAGIC ```
# MAGIC **SELECT句**…取得したいカラム名を指定(全てのカラムを取り出したい場合にはアスタリスク(*)を使用)<br>
# MAGIC **FROM句**…入力テーブル名を指定

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --まずは全てのデータを取得してみます。
# MAGIC SELECT 
# MAGIC     * 
# MAGIC FROM 
# MAGIC     sample_data
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --次に商品名(item_name)と商品価格(price)のカラムのデータのみを取得してみます。
# MAGIC SELECT 
# MAGIC     item_name,
# MAGIC     price
# MAGIC FROM
# MAGIC     sample_data
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ※複数のカラムをSELECT句に置く場合、カラムとカラムの間にコンマ(,)を入れる点に注意です。

# COMMAND ----------

# MAGIC %md
# MAGIC ###・カラムの追加

# COMMAND ----------

# MAGIC %md
# MAGIC 数値型のカラムであれば、カラム名を算術演算でそのまま使用する事が出来ます。<br>
# MAGIC データフレームの例と同様に、商品価格(price)と販売個数(unit_sales)を掛ける事で、総売上を計算します。

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC     *,
# MAGIC     price * unit_sales
# MAGIC FROM 
# MAGIC     sample_data
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC 特に指定がない場合、新しく作成したカラムには記載した計算式がそのまま付けられます。<br><br>
# MAGIC 計算式の横に「AS + カラム名」を置くことで、<br>
# MAGIC 新しく作成したカラムに名前を付ける事が出来ます。

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC     *,
# MAGIC     price * unit_sales AS total_sales
# MAGIC FROM 
# MAGIC     sample_data
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ###・データの抽出

# COMMAND ----------

# MAGIC %md
# MAGIC WHERE句を使うことで、特定の条件に合ったデータのみを抽出する事が出来ます。<br>
# MAGIC ・構文
# MAGIC ```
# MAGIC SELECT 
# MAGIC     ~~~
# MAGIC FROM
# MAGIC     ~~~
# MAGIC WHERE
# MAGIC     ~~~
# MAGIC ;
# MAGIC ```
# MAGIC **WHERE句**内では条件式を指定します。(例：「category = "fluit"」, 「price > 100」)

# COMMAND ----------

# MAGIC %md
# MAGIC 販売個数が10よりも多い商品のデータのみを抽出します。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     *
# MAGIC FROM 
# MAGIC     sample_data
# MAGIC WHERE
# MAGIC     unit_sales > 10
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ###・データのソート

# COMMAND ----------

# MAGIC %md
# MAGIC ORDER BY句を使うことで、データの表示順を変更する事が出来ます。<br>
# MAGIC ・構文
# MAGIC ```
# MAGIC SELECT 
# MAGIC     ~~~
# MAGIC FROM
# MAGIC     ~~~
# MAGIC ORDER BY
# MAGIC     ~~~
# MAGIC ;
# MAGIC ```
# MAGIC ORDER BY句内で、並び替える基準のカラムと、並び替える順序(昇順(ASC)、降順(DESC))の指定を行います。<br>
# MAGIC 例：```ORDER BY price DESC```<br>
# MAGIC ※並び替えの順序を指定しない場合には、昇順で並び替えがなされます。

# COMMAND ----------

# MAGIC %md
# MAGIC 商品価格(price)の低い順番にデータの並び替えを行います。

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- ※ORDER BY句のASCはつけなくても出力結果は同じになります。
# MAGIC SELECT
# MAGIC     *
# MAGIC FROM
# MAGIC     sample_data
# MAGIC ORDER BY
# MAGIC     price ASC
# MAGIC ;
