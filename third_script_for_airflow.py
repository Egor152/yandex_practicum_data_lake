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
    output_base_path = str(sys.argv[2]) # third_mart

    spark = SparkSession.builder \
        .master("yarn") \
        .appName("project_third_script") \
        .getOrCreate()

    def get_user_city_info(df_path):
        new_events = spark.read.parquet(f'{df_path}')
        geo_csv = spark.read.csv('/user/iamegoor/data/geo_2.csv', sep=";", inferSchema=True, header=True)

        new_events = new_events.filter(F.col('event_type') == 'message')

        last_channel = new_events.select(F.col('event.subscription_channel').alias('channel'),
                                         F.col('event.user').alias('user_id')).distinct()
        channel_subscriptions = last_channel.join(last_channel.withColumnRenamed('user_id', 'user_id2'), ['channel'],
                                                  'inner') \
            .filter('user_id < user_id2')

        m_from_m_to = new_events.filter(
            (F.col('event_type') == 'message') & (F.col('event.message_from').isNotNull()) & (
                F.col('event.message_to').isNotNull())) \
            .select('event.message_from', 'event.message_to')

        sender_receiver_ids = m_from_m_to.select('message_from', 'message_to')

        receiver_sender_ids = m_from_m_to.select('message_to', 'message_from')

        contacters = sender_receiver_ids.union(receiver_sender_ids)

        didnt_communicate = channel_subscriptions.join(contacters,
                                                       on=(channel_subscriptions['user_id'] == contacters[
                                                           'message_from'])
                                                          & (channel_subscriptions['user_id2'] == contacters[
                                                           'message_to']),
                                                       how='left_anti')

        didnt_communicate = didnt_communicate.select('user_id', 'user_id2')

        last_message_coordinate = new_events.filter(
            (F.col('event_type') == 'message') & (F.col('event.message_from').isNotNull())) \
            .selectExpr('event.message_from', 'event.message_ts', 'lat as message_lat', 'lon as message_long')

        window_for_last_message = Window().partitionBy(["message_from"]).orderBy(F.col("message_ts").desc())

        last_message_coordinate = last_message_coordinate.withColumn('rank',
                                                                     F.row_number().over(window_for_last_message)) \
            .where("rank == 1") \
            .drop('rank')

        last_message_coordinate = last_message_coordinate.withColumn('message_sender', F.col('message_from'))

        users_coordinates = didnt_communicate.join(last_message_coordinate.alias('df1'),
                                                   on=didnt_communicate['user_id'] == F.col('df1.message_from'),
                                                   how='inner') \
            .withColumnRenamed('message_lat', 'lat_user1') \
            .withColumnRenamed('message_long', 'long_user1') \
            .withColumnRenamed('message_ts', 'message_ts_user1') \
            .join(last_message_coordinate.alias('df2'),
                  on=didnt_communicate['user_id2'] == F.col('df2.message_sender'),
                  how='inner') \
            .withColumnRenamed('message_lat', 'lat_user2') \
            .withColumnRenamed('message_long', 'long_user2') \
            .withColumnRenamed('message_ts', 'message_ts_user2')

        # айдишники юзеров, их последние координаты и даты сообщений
        users_coordinates = users_coordinates.select('user_id', 'lat_user1', 'long_user1', 'message_ts_user1',
                                                     'user_id2', 'lat_user2', 'long_user2', 'message_ts_user2')

        # расчет дистанции юзеров друг от друга
        r = 6371
        formula_df = users_coordinates.withColumn('square_sinus_lat',
                                                  F.pow(F.sin(
                                                      (users_coordinates['lat_user1'] - users_coordinates[
                                                          'long_user2']) / 2),
                                                      F.lit(2))) \
            .withColumn('square_sinus_long',
                        F.pow(F.sin((users_coordinates['long_user1'] - users_coordinates['long_user2']) / 2), F.lit(2))) \
            .withColumn('cos_lat_user1', F.cos(users_coordinates['lat_user1'])) \
            .withColumn('cos_lat_user2', F.cos(users_coordinates['lat_user2']))

        distance_df = formula_df.withColumn('distance', 2 * r * F.asin(
            F.sqrt(F.col('square_sinus_lat') + F.col('cos_lat_user1') * F.col('cos_lat_user2') * F.col(
                'square_sinus_long'))))

        result_distance_between_users = distance_df.select('user_id', 'lat_user1', 'long_user1', 'message_ts_user1',
                                                           'user_id2', 'lat_user2', 'long_user2', 'message_ts_user2',
                                                           'distance') \
            .filter(F.col('distance') < 1)

        # собираю данные для витрины в одном месте

        data_for_result_formula = result_distance_between_users.crossJoin(geo_csv)

        data_for_result_formula = data_for_result_formula.selectExpr('user_id', 'lat_user1', 'long_user1',
                                                                     'message_ts_user1',
                                                                     'user_id2', 'id as zone_id', 'city',
                                                                     'lat as geo_lat',
                                                                     'lng as geo_long', 'timezone')

        # расчеты дистанции для опеределения города
        r = 6371
        result_formula_df = data_for_result_formula.withColumn('square_sinus_lat',
                                                               F.pow(F.sin((data_for_result_formula['geo_lat'] -
                                                                            data_for_result_formula['lat_user1']) / 2),
                                                                     F.lit(2))) \
            .withColumn('square_sinus_long',
                        F.pow(F.sin((data_for_result_formula['geo_long'] - data_for_result_formula['long_user1']) / 2),
                              F.lit(2))) \
            .withColumn('cos_geo_lat', F.cos(data_for_result_formula['geo_lat'])) \
            .withColumn('cos_user_lat', F.cos(data_for_result_formula['lat_user1']))

        result_distance_df = result_formula_df.withColumn('result_distance', 2 * r * F.asin(
            F.sqrt(
                F.col('square_sinus_lat') + F.col('cos_geo_lat') * F.col('cos_user_lat') * F.col('square_sinus_long'))))

        window_for_result_formula = Window().partitionBy('user_id', 'user_id2').orderBy(F.asc('result_distance'))

        # датафрейм для расчета определения города юзеров
        result_distance_w_rn = result_distance_df.selectExpr('user_id', 'user_id2', 'message_ts_user1', 'zone_id',
                                                             'result_distance',
                                                             'timezone') \
            .withColumn('row_number', F.row_number().over(window_for_result_formula)) \
            .filter(F.col('row_number') == 1)

        result = result_distance_w_rn.withColumn("local_time",
                                                 F.from_utc_timestamp(F.col("message_ts_user1"), F.col("timezone"))) \
            .withColumn('processed_dttm', F.current_timestamp())

        result = result.selectExpr('user_id as user_left',
                                   'user_id2 as user_right',
                                   'processed_dttm',
                                   'zone_id',
                                   'local_time')
        return result

    data = get_user_city_info(df_path)

    writer = partition_writer(data)
#запись файла в директорию data_marts. В output_base_path передается название витрины
    writer.save(f'/user/iamegoor/data/data_marts/{output_base_path}')


if __name__ == "__main__":
    main()