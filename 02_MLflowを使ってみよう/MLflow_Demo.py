# Databricks notebook source
# MAGIC %md
# MAGIC ## MLflow動作デモ
# MAGIC
# MAGIC 本デモにおいては、MLflowを用いてモデル作成およびその記録をしていく流れを説明します。<br>
# MAGIC データにはデータ分析入門としてよく使われるタイタニック号のデータを使います。<br>
# MAGIC クラスターはMLと付いているものをご利用下さい。
# MAGIC
# MAGIC ちなみに下記プログラムは機械学習研修のpythonプログラムをほぼ丸パクりしています。

# COMMAND ----------

# import
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from sklearn.linear_model import LogisticRegression

%matplotlib inline

pd.set_option(   'display.max_rows', 100)    # 行の表示を増やす
pd.set_option('display.max_columns', 100)    # 列の表示を増やす

# COMMAND ----------

# mlflowの設定
import mlflow
mlflow.autolog() 

# COMMAND ----------

titanic=pd.read_csv("input_files/train.csv")

# COMMAND ----------

# Step2：数値変数の処理
#   ⇒ 本処理ではAgeとFareの欠損を平均値で埋める
Age_mean=titanic['Age'].mean()    # 平均を算出
print(Age_mean)
titanic[['Age']]=titanic[['Age']].fillna(Age_mean)    # fillnaでNaNを(平均で)埋める

Fare_mean=titanic['Fare'].mean()    # 平均を算出
print(Fare_mean)
titanic[['Fare']]=titanic[['Fare']].fillna(Fare_mean)    # fillnaでNaNを(平均で)埋める

# COMMAND ----------

# Step3：文字変数の処理
#   ⇒ 本処理ではPassengerIdとNameは使用しない
#   ⇒ TicketとCabinの先頭1文字をカテゴリ変数として使用する
titanic['Ticket_initial1']=titanic['Ticket'].str[0]
titanic['Cabin_initial1']=titanic['Cabin'].str[0]

# COMMAND ----------

# One Hot Encoding
#   => dummy_na   : NaNをダミー変数化するか(True=する／False=しない)
#      drop_first : 1カラム目をDropするか(True=する／False=しない)
titanic=pd.get_dummies(titanic,
                       dummy_na=True,
                       drop_first=False,
                       columns=['Pclass', 'Sex', 'Cabin_initial1', 'Embarked', 'Ticket_initial1'])

# COMMAND ----------

# Step1：基礎統計
#   ⇒ 基本的にdescribeで良いが、95%点とかは算出されないため追加する
var_num=['Age', 'SibSp', 'Parch', 'Fare']    # 数値変数をリスト化
per_lst=[0.002, 0.0025, 0.01, 0.25, 0.5, 0.75, 0.95, 0.975, 0.99, 0.995, 0.9975, 0.998]    # パーセンタイル点をリスト化(余計なものあり)

describe_num=titanic[var_num].describe(percentiles=per_lst).T    # 数値変数に関してdescribe & 転置(.T)

# dataframeの調整
describe_num=describe_num.reset_index(drop=False)                # index(変数名)を列に変換
describe_num=describe_num.rename(columns={'index':'var_name'})    # カラム名をリネーム

# COMMAND ----------

# Step1 : 外れ値の処理
# 関数定義
# 外れ値を上下0.25%点の値で上書きする(正規分布版)
def hosei_normal(df, des, var):
    tmp=des[des['var_name']==var]    # 対象の変数のdescribeを抽出
    df[var]=np.where(df[var]< tmp['0.25%'].values[0],  tmp['0.25%'].values[0], df[var])    # 下側0.25%を補正
    df[var]=np.where(df[var]>tmp['99.75%'].values[0], tmp['99.75%'].values[0], df[var])    # 上側0.25%を補正
    return df

# 外れ値を上0.5%点の値で上書きする(ポアソン分布版)
def hosei_poisson(df, des, var):
    tmp=des[des['var_name']==var]    # 対象の変数のdescribeを抽出
    df[var]=np.where(df[var]>tmp['99.5%'].values[0], tmp['99.5%'].values[0], df[var])      # 上側0.5%を補正
    return df

# COMMAND ----------

# 補正実行
titanic=hosei_normal( df=titanic, des=describe_num, var='Age')
titanic=hosei_poisson(df=titanic, des=describe_num, var='SibSp')
titanic=hosei_poisson(df=titanic, des=describe_num, var='Parch')
titanic=hosei_poisson(df=titanic, des=describe_num, var='Fare')

# COMMAND ----------

# Step2 : 不要な変数をDropして相関係数を確認する(PassengerIdはkeyとして残しておく)
drop_var_list=['Pclass_3.0',             # マルチコ対策でDrop
               'Pclass_nan',             # 値が全て0なのでDrop
               'Sex_male',               # マルチコ対策でDrop
               'Sex_nan',                # 値が全て0なのでDrop
               'Cabin_initial1_nan',     # マルチコ対策でDrop
               'Embarked_S',             # マルチコ対策でDrop
               'Ticket_initial1_W',      # マルチコ対策でDrop
               'Ticket_initial1_nan',    # 値が全て0なのでDrop
               'Name',                   # 名前はDrop
               'Cabin',                  # Cabinは先頭1文字を抽出しているのでDrop
               'Ticket',                 # Ticketは先頭1文字を抽出しているのでDrop
               ]

titanic=titanic.drop(drop_var_list, axis=1)

# COMMAND ----------

train, test=train_test_split(titanic,
                             test_size=0.2,
                             random_state=1,
                             stratify=titanic[['Survived']])

# COMMAND ----------

# 確認
print(titanic.shape)
print(train.shape)
print(test.shape)
print("======================")
print(titanic['Survived'].value_counts(normalize='index'))
print(train['Survived'].value_counts(normalize='index'))
print(test['Survived'].value_counts(normalize='index'))

# COMMAND ----------

# Step1 : Hold Out
#   分析データをtrain/testに分割したがそれぞれ説明変数と目的変数が1つのデータになっているので
#   それらをX(説明変数)とy(目的変数)に分割する。
train_X=train.drop(['Survived'], axis=1)
train_y=train[['PassengerId', 'Survived']]

test_X=test.drop(['Survived'], axis=1)
test_y=test[['PassengerId', 'Survived']]

# COMMAND ----------

print(train_X.info())
print("============================================")
print(train_y.info())
print("============================================")
print(test_X.info())
print("============================================")
print(test_y.info())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### ハイパーパラメータチューニング箇所ですが、時間がかかるのでコメントアウトしています。<br>50分ほどかかるので、全員でやると終わらなくなります。

# COMMAND ----------

# ロジスティック回帰モデルの探索したいパラメータを(リスト型で)用意
param_C=[0.01, 0.1, 1, 10, 100]
param_intercept_scaling=[0.01, 0.1, 1, 10, 100]
param_l1_ratio=[0.1, 0.3, 0.5, 0.7, 0.9]
param_penalty=['l1', 'l2', 'none', 'elasticnet']
param_solver=['newton-cg', 'sag', 'saga', 'lbfgs', 'liblinear']
param_class_weight=['balanced', None]
param_tol=[0.0001]
param_fit_intercept=[True]
param_dual=[False]
param_max_iter=[3000]
param_random_state=[1]

# 辞書型でパラメータをまとめる
param_grid={'C':param_C,
            'intercept_scaling':param_intercept_scaling,
            'l1_ratio':param_l1_ratio,
            'penalty':param_penalty,
            'solver':param_solver,
            'class_weight':param_class_weight,
            'tol':param_tol,
            'fit_intercept':param_fit_intercept,
            'dual':param_dual,
            'max_iter':param_max_iter,
            'random_state':param_random_state
           }

# ロジスティック回帰モデルのインスタンスを作成
LR=LogisticRegression()

# GridSearchCVを定義する(分割数=5とする)
gscv=GridSearchCV(estimator=LR, param_grid=param_grid, cv=5, n_jobs=-1, verbose=0)

# GridSearchCVを実行する(モデルを作成する)
# gscv.fit(train_X.drop('PassengerId', axis=1), np.ravel(train_y.drop('PassengerId', axis=1)))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 上記の元プログラムから一部のサーチを短絡したものになります。<br>1人で実行すると6分程度かかりました。

# COMMAND ----------

# ロジスティック回帰モデルの探索したいパラメータを(リスト型で)用意
param_C=[0.01, 0.1, 1, 10, 100]
param_intercept_scaling=[0.01, 0.1, 1, 10, 100]
param_penalty=['l2', 'none', 'elasticnet']
param_solver=['newton-cg', 'sag', 'saga', 'lbfgs', 'liblinear']
param_class_weight=['balanced', None]
param_tol=[0.0001]
param_fit_intercept=[True]
param_dual=[False]
param_max_iter=[3000]
param_random_state=[1]

# 辞書型でパラメータをまとめる
param_grid={'C':param_C,
            'intercept_scaling':param_intercept_scaling,
            'penalty':param_penalty,
            'solver':param_solver,
            'class_weight':param_class_weight,
            'tol':param_tol,
            'fit_intercept':param_fit_intercept,
            'dual':param_dual,
            'max_iter':param_max_iter,
            'random_state':param_random_state
           }

# ロジスティック回帰モデルのインスタンスを作成
LR=LogisticRegression()

# GridSearchCVを定義する(分割数=5とする)
gscv=GridSearchCV(estimator=LR, param_grid=param_grid, cv=5, n_jobs=-1, verbose=0)

# GridSearchCVを実行する(モデルを作成する)
gscv.fit(train_X.drop('PassengerId', axis=1), np.ravel(train_y.drop('PassengerId', axis=1)))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### sklearnのグリッドサーチはなぜか全件表示してくれない（mlflowと相性が悪いっぽい）ので、泥臭くループさせて記録していく方法も紹介します。<br>グリッドサーチと違って毎回呼び出すので、その分だけ時間が多くかかります。

# COMMAND ----------

from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

param_C=[0.01, 0.1, 1, 10, 100]
param_intercept_scaling=[0.01, 0.1, 1, 10, 100]
param_penalty=['l2', 'none', 'elasticnet']

with mlflow.start_run(run_name="lr_test") as run:
    for C in param_C:
        for intercept_scaling in param_intercept_scaling:
            for penalty in param_penalty:
                with mlflow.start_run(run_name="lr_test_child", nested=True) as run:
                    try:
                        print(f"C={C} intercept_scaling={intercept_scaling} penalty={penalty}")
                        model = LogisticRegression(C=C, intercept_scaling=intercept_scaling, penalty=penalty)
                        model.fit(train_X.drop('PassengerId', axis=1), train_y.drop('PassengerId', axis=1))
                        predictions = model.predict(test_X.drop('PassengerId', axis=1))
                        rmse = np.sqrt(mean_squared_error(test_y.drop('PassengerId', axis=1), predictions))
                        mlflow.log_metric("rmse", rmse) 
                        print(f"{rmse:5.3f} {run.info.run_id}")
                    except:
                        continue

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 上記セルで学習した結果については「エクスペリメント」タブから確認ができます。UIを好きに操作して色々な観点からモデルを観察してみましょう。
# MAGIC
# MAGIC tags."mlflow.runName" = "lr_test_child"
# MAGIC のような形式でRun Nameでフィルタリングできます。
