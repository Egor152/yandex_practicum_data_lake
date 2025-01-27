import sys
import pyspark
import datetime
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import Window
import pyspark.sql.functions as F
import os
import os
import findspark

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'
findspark.init()


def partition_writer(candidates):
    return candidates.write.mode('overwrite').format('parquet')


def main():
    df_path = str(sys.argv[1])  # /user/master/data/geo/events
    output_base_path = str(sys.argv[2]) # first_mart

    spark = SparkSession.builder \
        .master("yarn") \
        .appName("project_first_script") \
        .getOrCreate()

    def get_user_city_info(df_path):
        new_events = spark.read.parquet(f'{df_path}')
        geo_csv = spark.read.csv('/user/iamegoor/data/geo_2.csv', sep=";", inferSchema=True, header=True)

        new_events = new_events.filter(F.col('event_type') == 'message')

        geo_csv = geo_csv.withColumn("geo_lat", F.regexp_replace("lat", ",", ".")) \
            .withColumn("geo_long", F.regexp_replace("lng", ",", ".")) \
            .drop('lat') \
            .drop('lng')

        message_info = new_events.select('event.message_id', 'event.message_from',
                                         F.col('event.message_ts').cast("Timestamp").alias('orig_message_ts'),
                                         F.col('event.datetime').cast("Timestamp").alias('orig_datetime'), 'lat', 'lon') \
            .filter(F.col('event.message_from').isNotNull())

        # здесь я делаю один атрибут datetime, где не будет нулловых значений, потому что в некоторых message_ts и datetime были нуллы
        message_info = message_info.withColumn('datetime',
                                               F.when(F.col("orig_message_ts").isNull(),
                                                      F.col("orig_datetime")).otherwise(
                                                   F.col("orig_message_ts"))) \
            .select('message_id',
                    'message_from',
                    F.col('lat').alias('message_lat'),
                    F.col('lon').alias('message_long'),
                    'datetime')

        joined_data = message_info.crossJoin(geo_csv)

        # расчеты формулы
        r = 6371
        formula_df = joined_data.withColumn('square_sinus_lat',
                                            F.pow(F.sin((joined_data['geo_lat'] - joined_data['message_lat']) / 2),
                                                  F.lit(2))) \
            .withColumn('square_sinus_long',
                        F.pow(F.sin((joined_data['geo_long'] - joined_data['message_long']) / 2), F.lit(2))) \
            .withColumn('cos_geo_lat', F.cos(joined_data['geo_lat'])) \
            .withColumn('cos_message_lat', F.cos(joined_data['message_lat']))

        distance_df = formula_df.withColumn('distance', 2 * r * F.asin(
            F.sqrt(F.col('square_sinus_lat') + F.col('cos_geo_lat') * F.col('cos_message_lat') * F.col(
                'square_sinus_long'))))

        window_for_result_distance = Window().partitionBy('message_id').orderBy(F.asc('distance'))

        # общий датафрейм с расстояниями сообщений от центров городов
        result_distance_w_rn = distance_df.selectExpr('message_id', 'message_from as user_id', 'datetime', 'city',
                                                      'distance',
                                                      'timezone') \
            .withColumn('row_number', F.row_number().over(window_for_result_distance)) \
            .filter(F.col('row_number') == 1)

        # Окна для оконных функций

        # общее окно
        base_window = Window().partitionBy(["user_id"])

        datetime_window = base_window.orderBy(F.col("datetime"))

        datetime_window_desc = base_window.orderBy(F.col("datetime").desc())

        # Расчеты данных для витрины

        # Актуальный город
        actual_city = result_distance_w_rn.withColumn("rank", F.row_number().over(datetime_window_desc)) \
            .where("rank == 1") \
            .selectExpr("user_id", "city as act_city")

        # считает путешествия юзера. В каком городе был
        travel_cities = result_distance_w_rn.withColumn("next_city", F.lead("city", 1, "finish").over(datetime_window)) \
            .withColumn("prev_city", F.lag("city", 1, "start").over(datetime_window)) \
            .filter("city != prev_city or city != next_city") \
            .withColumn("next_datetime",
                        F.coalesce(F.lead("datetime", 1).over(datetime_window),
                                   F.col("datetime") + F.expr('INTERVAL 24 HOURS'))) \
            .withColumn("prev_datetime",
                        F.coalesce(F.lag("datetime", 1).over(datetime_window), F.col("datetime"))) \
            .filter(F.col("city") != F.col("next_city")) \
            .withColumn("diff_in_days", F.datediff("next_datetime", "prev_datetime")) \
            .withColumn("travel_array", F.collect_list(F.col("city")).over(datetime_window)) \
            .withColumn("travel_count", F.count(F.col("city")).over(base_window)) \
            .withColumn("rnk", F.row_number().over(datetime_window_desc))

        # домашний адрес юзера
        home_city = travel_cities.filter("travel_count == 1 or diff_in_days > 27") \
            .withColumn("rnk", F.row_number().over(datetime_window_desc)) \
            .filter("rnk == 1").selectExpr("user_id", "city as home_city")

        # локальное время
        travel_cities = travel_cities.filter("rnk == 1") \
            .withColumn("local_time",
                        F.from_utc_timestamp(F.col("datetime"), F.col("timezone"))) \
            .select("user_id", "travel_count", "travel_array", "local_time", "timezone")

        # витрина
        result_travel_cities = travel_cities.join(actual_city, ["user_id"], "left") \
            .join(home_city, ["user_id"], "left") \
            .select(travel_cities.user_id, actual_city.act_city,
                    home_city.home_city, travel_cities.timezone,
                    travel_cities.travel_count, travel_cities.travel_array,
                    travel_cities.local_time)

        return result_travel_cities

    data = get_user_city_info(df_path)

    writer = partition_writer(data)
#запись файла в директорию data_marts(она создалась, когда я записал первую витрину). В output_base_path передается название витрины
    writer.save(f'/user/iamegoor/data/data_marts/{output_base_path}')


if __name__ == "__main__":
    main()