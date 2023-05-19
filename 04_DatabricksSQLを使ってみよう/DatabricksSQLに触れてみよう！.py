# Databricks notebook source
# MAGIC %md
# MAGIC #目標
# MAGIC ・DatabricksSQLの環境でSQLクエリを実行できるようになる<br>
# MAGIC ・SQLクエリの実行結果を可視化する事が出来るようになる<br>
# MAGIC ・可視化した実行結果をダッシュボードに貼り付けて表示する事が出来るようになる

# COMMAND ----------

# MAGIC %md
# MAGIC 初めに「**SQLエンドポイント**」タブを開き、<br>
# MAGIC 研修で使用する**Sample_Endpoint**という名前のエンドポイントが実行中になっていることを確認してみてください。

# COMMAND ----------

# MAGIC %md
# MAGIC 本研修では、DatabricksSQLにもともと用意されている「ニューヨーク市内のタクシー交通」のサンプルデータを使用します。<br>
# MAGIC まずはサンプルデータがどんなデータなのか確認するため、「**データ**」タブを開いてみましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC 以下の手順に従って、サンプルデータを確認します。<br><br>
# MAGIC ①プルダウンからカタログを**samples**に設定<br>
# MAGIC ②プルダウンからデータベースを**nycctaxi**に設定<br>
# MAGIC ③「**trips**」というテーブル名が下に表示されている事を確認し、テーブル名をクリックすると、<br>
# MAGIC 　右側にテーブルの概要が表示されます。<br><br>
# MAGIC
# MAGIC <img src="/files/images/data_check.png">

# COMMAND ----------

# MAGIC %md
# MAGIC テーブルのスキーマとカラムの説明について、下にまとめておきます。
# MAGIC
# MAGIC |  カラム名  |  データ型  |  説明  |
# MAGIC | ---- | ---- | ---- | 
# MAGIC |  tpep_pickup_datetime  |  timestamp  |  乗客を乗せた時刻  |
# MAGIC |  tpep_dropoff_datetime  |  timestamp  |  乗客を降ろした時刻  |
# MAGIC |  trip_distance  |  double  |  タクシーの走行距離(単位：マイル)  |
# MAGIC |  fare_amount  |  double  |  運賃(単位：USドル)  |
# MAGIC |  pickup_zip  |  int  |  乗客を乗せた場所のzipコード(郵便番号)  |
# MAGIC |  dropoff_zip  |  int  |  乗客を降ろした場所のzipコード  |
# MAGIC
# MAGIC (補足)<br>
# MAGIC timestamp型…**日付と時刻がセット**になって格納されるデータ型(例：2016-02-14T16:52:13)　※日付と時刻の間の**T**はタブを表す
# MAGIC
# MAGIC <br>サンプルデータについては自身の環境で開いて確認してみてください。

# COMMAND ----------

# MAGIC %md
# MAGIC それではこちらのテーブルをもとに、データ操作を実行して見ましょう。<br>
# MAGIC 「**SQLエディタ**」のタブを開いてください。

# COMMAND ----------

# MAGIC %md
# MAGIC 画面中央部の **`新規クエリを作成する`** という青文字をクリックして、クエリの入力画面を開きます。<br>
# MAGIC まずは簡単なクエリから実行してみましょう。以下のクエリをそのままコピー&ペーストして実行ボタンを押してください。
# MAGIC ```
# MAGIC SELECT
# MAGIC     *
# MAGIC FROM 
# MAGIC     samples.nyctaxi.trips
# MAGIC LIMIT 
# MAGIC     10
# MAGIC ;
# MAGIC ```
# MAGIC **※注意点**<br>
# MAGIC FROM句の書き方が先程のノートブックでのSQL実行とは異なります。<br>
# MAGIC 読み込むテーブル名を記載する際には、以下の文法に従う必要があります。<br>
# MAGIC ``<カタログ名>.<データベース名>.<テーブル名>``<br><br>
# MAGIC 今回、カタログ名は **`samples`**  データベース名は **`nyctaxi`** テーブル名は **`trips`** となっており、上記の書き方となっています。

# COMMAND ----------

# MAGIC %md
# MAGIC 画面の右側に **`スキーマブラウザ`** という画面が表示されているかと思います。(下図の赤枠部分)<br><br>
# MAGIC そちらに、先程データの確認作業を行ったときと同様にカタログ名とデータベース名を入力すると、<br>
# MAGIC データベース内のテーブルが確認できます。テーブル名をクリックすると、テーブル内のカラム名も表示されます。<br><br>
# MAGIC スキーマブラウザのテーブル名やデータベース名にマウスのカーソルを合わせると、「 **``>>``** 」というアイコンが右に表示されます。<br>
# MAGIC このアイコンをクリックすると、テーブル名やカラム名をクエリに挿入する事が出来ます。
# MAGIC <img src="/files/images/SQL_editor.png">

# COMMAND ----------

# MAGIC %md
# MAGIC 試しに、この挿入機能を利用してクエリの作成をしてみましょう。<br>
# MAGIC ※新しいクエリを追加する際には、クエリ名の隣にある「**＋**」ボタンにカーソルを合わせ、 **`新規クエリを作成`** をクリックします。

# COMMAND ----------

# MAGIC %md
# MAGIC **<練習1>**<br>
# MAGIC 以下の<>部分に、テーブル名、カラム名を挿入してクエリを完成させてください。<br>
# MAGIC 指定カラムの部分には、日本語の説明に合ったカラムを挿入してください。<br>
# MAGIC (※練習問題の解答については別ノートブック **「DatabricksSQL_練習問題解答例」** に載せております。)
# MAGIC ```
# MAGIC SELECT
# MAGIC     <走行距離>,
# MAGIC     <運賃>
# MAGIC FROM
# MAGIC     <テーブル名>
# MAGIC ORDER BY 
# MAGIC     <走行距離> DESC
# MAGIC LIMIT
# MAGIC     10
# MAGIC ;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC 次の作業の前に、これまでの復習を含め、<br>
# MAGIC SQLのコード作成の練習を行っていきます。

# COMMAND ----------

# MAGIC %md
# MAGIC **<練習2>**<br>
# MAGIC 以下の条件を満たすデータを取得するSQLを実行してください。<br>
# MAGIC ・すべてのカラムを表示します。<br>
# MAGIC ・乗客を乗せた場所のZIPコードが"10110"<br>
# MAGIC ・乗客を降ろした場所のZIPコードが"10282"<br><br>
# MAGIC ※複数の条件式を記述する場合には **`AND`** を使って条件式を繋ぐことが出来ます。<br>
# MAGIC ```
# MAGIC WHERE 
# MAGIC     <カラム名> == ○○○ AND
# MAGIC     <カラム名> == △△△
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **<練習3>**<br>
# MAGIC 以下の条件を満たすデータを取得するSQLを実行してください。<br>
# MAGIC ・`tpep_pickup_datetime` と`tpep_dropoff_datetime`のカラム内のデータを、時刻を除いた日付だけのデータに変換し、<br>
# MAGIC 　それぞれ`tpep_pickup_date`、`tpep_dropoff_date`という名前の新しいカラムを作成してください。(例："2016-02-14")<br><br>
# MAGIC ・上記2カラムと、走行距離(trip_distance)、運賃(fare_amouont)を合わせた、計4カラムを出力します。<br><br>
# MAGIC ・`tpep_pickup_date`が`"2016-02-14"`の日付のデータのみ抽出してください。<br><br>
# MAGIC ※timestamp型のカラムを日付型(date型)に変換するには **`DATE()`** 関数が必要となります。<br>
# MAGIC ```
# MAGIC SELECT
# MAGIC     DATE(<カラム名>) AS <新しいカラム名>
# MAGIC ```
# MAGIC ※作成した新しいカラムを条件式で使用したい場合には **`サブクエリ`** を使用する必要があります。

# COMMAND ----------

# MAGIC %md
# MAGIC これからダッシュボードの作成に入ります。<br>
# MAGIC 「ダッシュボード」タブを開き、画面右上の **`ダッシュボードを作成`** をクリックしてください。<br>
# MAGIC ダッシュボードの名前は「<社員番号>_<自分の苗字>_nyctaxi」で設定してください。(例：121025_Sasaki_nyctaxi)

# COMMAND ----------

# MAGIC %md
# MAGIC 白紙の状態のダッシュボードが作成できました。<br>
# MAGIC これからこのダッシュボードを可視化したデータで埋めていきます。<br>
# MAGIC 再度「SQLエディタ」タブに戻ってください。

# COMMAND ----------

# MAGIC %md
# MAGIC まずはデータの全件数を取得し、結果を可視化していきます。<br>
# MAGIC 以下のクエリをコピー&ペーストして実行してみてください。
# MAGIC ```
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_trips
# MAGIC FROM
# MAGIC     samples.nyctaxi.trips
# MAGIC ;
# MAGIC ```
# MAGIC ※`COUNT(*)`…テーブル内の全データの件数を取得する関数

# COMMAND ----------

# MAGIC %md
# MAGIC 実行結果が表示されたら、画面中央の右側にある **`ビジュアライゼーションを追加`** をクリックします。<br><br>
# MAGIC 以下の手順でデータの可視化を行います。<br>
# MAGIC ①画面一番上の名前を`total_trips`に変更<br>
# MAGIC ②`Visualization Type`を **`counter`** に設定<br>
# MAGIC ③保存をクリック<br><br>
# MAGIC 操作が完了すると、実行結果の画面に可視化された画像が表示されているはずです。

# COMMAND ----------

# MAGIC %md
# MAGIC 作成した画像をダッシュボードに貼り付けていきます。<br>
# MAGIC 以下の操作を行ってください。<br>
# MAGIC ①画面下の縦3点リーダーをクリックして、 **`ダッシュボードに追加`** をクリック<br>
# MAGIC ②プルダウンより、先程作成した自分の名前のダッシュボードを選択して **`保存して追加`** をクリック<br><br>
# MAGIC 「ダッシュボード」タブを開き、作成した画像が自分のダッシュボードに追加されている事を確認してください。

# COMMAND ----------

# MAGIC %md
# MAGIC 続いて、タクシーの走行距離と運賃の関連性を調べるために**散布図**を作成していきます。<br>
# MAGIC 以下のクエリをコピー&ペーストして実行してください。
# MAGIC ```
# MAGIC SELECT
# MAGIC     trip_distance,
# MAGIC     fare_amount
# MAGIC FROM
# MAGIC     samples.nyctaxi.trips
# MAGIC ;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **`ビジュアライゼーションを追加`** をクリックして散布図を作成します。<br><br>
# MAGIC 以下の操作を実行してください。<br>
# MAGIC ①画面一番上の名前を`Scatter_Graph`に変更<br>
# MAGIC ②`Visualization Type`を **`Scatter`** に設定<br>
# MAGIC ③`X column`を **`trip_distance`** に設定<br>
# MAGIC ④`Y column`を **`fare_amount`** に設定<br>
# MAGIC ⑤保存をクリック<br><br>
# MAGIC 操作が完了したら、自分のダッシュボードに作成した散布図を追加します。<br>
# MAGIC ダッシュボードへの貼り付け手順は先程と同様です。

# COMMAND ----------

# MAGIC %md
# MAGIC 最後に、どの経路に乗客が集中しているか確認できる**棒グラフ**を作成します。<br>
# MAGIC 以下のクエリをコピー&ペーストして実行してください。
# MAGIC ```
# MAGIC SELECT
# MAGIC     route,
# MAGIC     COUNT(*) AS freq_route
# MAGIC FROM
# MAGIC     (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CONCAT(pickup_zip, '-', dropoff_zip) AS route
# MAGIC     FROM
# MAGIC         samples.nyctaxi.trips
# MAGIC     )
# MAGIC GROUP BY
# MAGIC     route
# MAGIC ORDER BY
# MAGIC     freq_route DESC
# MAGIC LIMIT 
# MAGIC     10
# MAGIC ;
# MAGIC ```
# MAGIC こちらのSQLは、乗客数の上位10件の経路を多い順に表示するものとなっています。<br><br>
# MAGIC いくつか今回の研修で扱わなかった関数等が存在するため、下記で補足します。<br>
# MAGIC ・CONCAT()関数<br>
# MAGIC 文字列と文字列をくっつける関数です。<br>
# MAGIC 本処理では乗車場所と降車場所のZIPコードをくっつける事で**経路**を表す新しいカラム **`route`** を作成しています。<br><br>
# MAGIC ・GROUP BY句<br>
# MAGIC 同じ値を持つグループを一つにまとめる役割を持っています。<br>
# MAGIC  **<練習2>** の実行結果のように、本テーブルには経路が同じデータが複数存在する場合があります。<br>
# MAGIC これらのデータを一つにまとめ、それぞれの経路に該当するデータの数を上の`COUNT(*)`で集計する事で**乗客数**を表示する事が可能になります。

# COMMAND ----------

# MAGIC %md
# MAGIC **`ビジュアライゼーションを追加`** をクリックして棒グラフを作成します。<br><br>
# MAGIC 以下の操作を実行してください。<br>
# MAGIC ①画面一番上の名前を`Bar_Graph`に変更<br>
# MAGIC ②`Horizontal Chart`のチェックを解除<br>
# MAGIC ③`Visualization Type`を **`Bar`** に設定<br>
# MAGIC ④`X column`を **`route`** に設定<br>
# MAGIC ⑤`Y column`を **`freq_route`** に設定<br>
# MAGIC ⑥`X Axis`のタブに移動し、`Sort Values`をオフにします。<br>
# MAGIC ⑦保存をクリック<br><br>
# MAGIC 操作が完了したら、自分のダッシュボードに作成した棒グラフを追加してください。

# COMMAND ----------

# MAGIC %md
# MAGIC 以上でダッシュボードの作成は終了となります。<br>
# MAGIC 最後に作成したダッシュボードを確認してみましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC 可視化したデータが若干不揃いな配置になってしまっているので。配置を変更します。<br>
# MAGIC ダッシュボード右上の縦3点リーダーをクリックし、 **`編集`** をクリックしてください。<br><br>
# MAGIC 編集画面では、可視化したデータをドラッグして移動させることが可能です。<br>
# MAGIC 以下の画像のように配置を変更してください。配置移動が完了したら、 **`編集完了`** をクリックしてください。
# MAGIC <img src="/files/images/dashboard.png">

# COMMAND ----------

# MAGIC %md
# MAGIC 以上で「DatabricksSQLに触れてみよう」の演習は終了となります。<br>
# MAGIC お疲れ様でした！
