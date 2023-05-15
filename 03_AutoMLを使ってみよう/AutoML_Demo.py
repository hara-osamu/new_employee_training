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
# MAGIC |  項目名  |  値  | 説明 |
# MAGIC | ---- | ---- | ---- |
# MAGIC |  クラスター  |  このNotebookの実行に利用したもの  | MLクラスタである必要がある |
# MAGIC |  機械学習の問題のタイプ  |  分類  | 「0 = 死亡, 1 = 生存」の分類モデルを作成する |
# MAGIC |  入力学習用データセット  |  ***_comture_com_automl_demo.titanic_training  | 上で作成した自分のデータベース内のデータを参照する |
# MAGIC |  予測ターゲット  |  Survived  | 今回の目的変数（生死）を指定する |
# MAGIC |  エクスペリメント名  |  Survived_titanic_training-yyyy_MM_dd-hh_mm  | デフォルトのままでOK |
# MAGIC
# MAGIC
# MAGIC <img src="/files/automl_demo/AutoML_Experiment_Setting.png">
# MAGIC
# MAGIC
# MAGIC 「高度な設定」を展開すると下記の設定項目が追加で出てきます。  
# MAGIC 今回はタイムアウトを10分に変更して実行しましょう。  
# MAGIC ※AutoMLは学習中の手動停止も可能です。途中で停止した場合、その時点までに学習したモデルについては利用が可能になります。
# MAGIC
# MAGIC | 項目名 | 説明 | デフォルト値 |
# MAGIC | ---- | ---- | ---- |
# MAGIC | 評価メトリクス  | モデルの優劣を評価するための指標値 | 回帰:R2<br>分類:F1<br>予測:SMAPE |
# MAGIC | トレーニングフレームワーク | 指定したフレームワーク内のアルゴリズムを学習に利用 | Scikit-learn,XGBoost,LightGBM |
# MAGIC | タイムアウト(分) | モデル学習を打ち切る時間/回数条件<br>指標が改善しなければこれを待たずに止まるし、手動での打ち切りも可能 | 120分 |
# MAGIC | 訓練/検証/テストデータセット分割用の時間列 | データ分割に使用する時間列を指定<br>時系列で連続するデータの場合、これを行わないと不当にいい精度が出かねない | - |
# MAGIC | ポジティブラベル | 二値分類問題のprecision,recall,f1の計算時に、どの値をポジティブとして計算するか。 | - |
# MAGIC | 中間データの保存場所 | AutoML実行時に作成される中間データの保存場所 | MLflowアーティファクト(≒DBFS内に自動生成) |
# MAGIC
# MAGIC
# MAGIC <img src="/files/automl_demo/AutoML_Experiment_Setting_Stoptime.png">

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
