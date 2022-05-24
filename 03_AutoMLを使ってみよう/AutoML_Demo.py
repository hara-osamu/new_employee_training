# Databricks notebook source
# MAGIC %md
# MAGIC # AutoML動作デモ
# MAGIC 
# MAGIC 本デモにおいては、データ分析入門としてよく使われるタイタニック号のデータを使ってのデータ分析がAutoMLを利用するとどうなるかを見ていきます。<br>
# MAGIC クラスターはMLと付いているものをご利用下さい。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 環境設定
# MAGIC 今回利用したいデータベースを作成します。
# MAGIC データベースは「(メールアドレス)_automl_demo」という名前で作成されます。

# COMMAND ----------

import re

course = "automl_demo"
username = spark.sql("SELECT current_user()").first()[0]
database = f"{re.sub('[^a-zA-Z0-9]', '_', username)}_{course}"

spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
spark.sql(f"USE {database}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## データの読み込み
# MAGIC Gitに事前にアップロードしてある各入力データを読み込みます。<br>

# COMMAND ----------

import pandas as pd
# タイタニックの学習データ
spark.createDataFrame(pd.read_csv("input_files/train.csv")).write.mode("overwrite").saveAsTable("titanic_training")
# 乳ガンの診断データセット 列名にスペースがあるがDatabricksのDeltaでは使えないので全部「_」に置換してから保存
pdf = pd.read_csv("input_files/breast_cancer.csv")
pdf.columns = [s.replace(' ', '_') for s in pdf.columns]
spark.createDataFrame(pdf).write.mode("overwrite").saveAsTable("breast_cancer")
# 離婚調査データセット
spark.createDataFrame(pd.read_csv("input_files/divorce_data.csv")).write.mode("overwrite").saveAsTable("divorce_data")
# 製品購入に関するデータセット
spark.createDataFrame(pd.read_csv("input_files/Social_Network_Ads.csv")).write.mode("overwrite").saveAsTable("Social_Network_Ads")
# 宇宙船タイタニック号の乗客に関するデータセット 列名にスペースがあるがDatabricksのDeltaでは使えないので全部「_」に置換してから保存
# 列名にドットがあるがAutoMLでは使えないので全部「_」に置換してから保存
pdf = pd.read_csv("input_files/spaceship-titanic.csv")
pdf.columns = [s.replace(' ', '_').replace('.', '_') for s in pdf.columns]
spark.createDataFrame(pdf).write.mode("overwrite").saveAsTable("spaceship_titanic")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 読み込んだデータの確認
# MAGIC 
# MAGIC 作成したデータベースや読み込んだデータについては左の「データ」タブから参照できます。
# MAGIC ※上記で作成した自分のデータベース（メールアドレス_automl_demo）を確認して下さい。
# MAGIC 
# MAGIC <img src="/files/automl_demo/Database.png" width="600">
# MAGIC 
# MAGIC テーブル名をダブルクリックすることで、データの内容を確認できます。
# MAGIC 
# MAGIC <img src="/files/automl_demo/titanic_training.png">

# COMMAND ----------

# MAGIC %md
# MAGIC ## AutoMLの実行
# MAGIC 
# MAGIC 左の「エクスペリメント」を選択し、上部の「AutoMLエクスペリメントを作成」をクリックします。
# MAGIC 
# MAGIC <img src="/files/automl_demo/Create_AutoML_Experiment.png">
# MAGIC 
# MAGIC 各項目を以下のように設定します。
# MAGIC その後、下部の「AutoMLを開始」をクリックします。
# MAGIC ※本資料作成用の実行時には6分ほどで完了しました。
# MAGIC |  項目名  |  値  | 説明 |
# MAGIC | ---- | ---- | ---- |
# MAGIC |  クラスター  |  このNotebookの実行に利用したもの  | MLクラスタである必要がある |
# MAGIC |  機械学習のタイプ  |  分類  | 「0 = 死亡, 1 = 生存」の分類モデルを作成する |
# MAGIC |  データセット  |  ***_comture_com_automl_demo.titanic_training  | 上で作成した自分のデータベース内のデータを参照する |
# MAGIC |  予測ターゲット  |  Survived  | 今回の目的変数（生死）を指定する |
# MAGIC |  エクスペリメント名  |  Survived_titanic_training-yyyy_MM_dd-hh_mm  | デフォルトのままでOK |
# MAGIC 
# MAGIC 
# MAGIC <img src="/files/automl_demo/AutoML_Experiment_Setting.png">
# MAGIC 
# MAGIC 
# MAGIC 「高度な設定」を展開すると下記の設定項目が追加で出てきますが、今回は特段の変更は不要です。
# MAGIC 
# MAGIC | 項目名 | 説明 | デフォルト値 |
# MAGIC | ---- | ---- | ---- |
# MAGIC | 評価メトリクス  | モデルの優劣を評価するための指標値 | 回帰:R2<br>分類:F1<br>予測:SMAPE |
# MAGIC | トレーニングフレームワーク | 指定したフレームワーク内のアルゴリズムを学習に利用 | Scikit-learn,XGBoost,LightGBM |
# MAGIC | 停止条件 | モデル学習を打ち切る時間/回数条件<br>指標が改善しなければこれを待たずに止まるし、手動での打ち切りも可能 | 60分/200回 |
# MAGIC | 訓練/検証/テストデータセット分割用の時間列 | データ分割に使用する時間列を指定<br>時系列で連続するデータの場合、これを行わないと不当にいい精度が出かねない | - |
# MAGIC | 中間データの保存場所 | AutoML実行時に作成される中間データの保存場所 | MLflowアーティファクト(≒DBFS内に自動生成) |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 結果の確認
# MAGIC 
# MAGIC 上記操作後にAutoMLの学習結果画面に自動で移行しているかと思います。<br>
# MAGIC 閉じてしまった等の場合、左の「エクスペリメント」タブをクリックすると表示されます。<br>
# MAGIC ※同じ名前のものが並んでいても、作成者で区別できます。
# MAGIC 
# MAGIC デフォルトで指定した評価メトリクス順に並んでいて、実行した中で優れていたモデルが一目で分かるようになっています。<br>
# MAGIC また、「最適なモデルのノートブックを表示」で最も精度が高かったモデル構築に用いられたプログラムを表示することができ、<br>
# MAGIC 「データ探索用ノートブックを表示」で事前のデータ探索に用いたノートブックを表示できます。<br>
# MAGIC （Source列からそれ以外のプログラムも表示できます。）
# MAGIC 
# MAGIC <img src="/files/automl_demo/AutoML_Result.png">
