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
    output_base_path = str(sys.argv[2]) # second_mart

    spark = SparkSession.builder \
        .master("yarn") \
        .appName("project_second_script") \
        .getOrCreate()

    def get_user_city_info(df_path):
        new_events = spark.read.parquet(f'{df_path}')
        geo_csv = spark.read.csv('/user/iamegoor/data/geo_2.csv', sep=";", inferSchema=True, header=True)

        new_events = new_events.filter(F.col('event_type') == 'message')

        geo_csv = geo_csv.withColumn("geo_lat", F.regexp_replace("lat", ",", ".")) \
            .withColumn("geo_long", F.regexp_replace("lng", ",", ".")) \
            .drop('lat') \
            .drop('lng')

        events_info = new_events.select('event.message_id', 'event.message_from',
                                        F.col('event.message_ts').cast("Timestamp").alias('message_ts'),
                                        'event.reaction_type', 'event.reaction_from',
                                        'event_type',
                                        F.col('event.datetime').cast("Timestamp").alias('event_ts'),
                                        F.col('lat').alias('event_lat'),
                                        F.col('lon').alias('event_long')) \
            .withColumn("event_id", F.monotonically_increasing_id())

        joined_data = events_info.crossJoin(geo_csv)

        r = 6371
        formula_df = joined_data.withColumn('square_sinus_lat',
                                            F.pow(F.sin((joined_data['geo_lat'] - joined_data['event_lat']) / 2),
                                                  F.lit(2))) \
            .withColumn('square_sinus_long',
                        F.pow(F.sin((joined_data['geo_long'] - joined_data['event_long']) / 2), F.lit(2))) \
            .withColumn('cos_geo_lat', F.cos(joined_data['geo_lat'])) \
            .withColumn('cos_event_lat', F.cos(joined_data['event_lat']))

        distance_df = formula_df.withColumn('distance', 2 * r * F.asin(
            F.sqrt(F.col('square_sinus_lat') + F.col('cos_geo_lat') * F.col('cos_event_lat') * F.col(
                'square_sinus_long'))))

        window_for_result_distance = Window().partitionBy('event_id').orderBy(F.asc('distance'))

        # общий датафрейм с расстояниями сообщений от центров городов
        result_distance_w_rn = distance_df.selectExpr('message_id',
                                                      'message_from',
                                                      'message_ts',
                                                      'reaction_type',
                                                      'reaction_from',
                                                      'event_id',
                                                      'event_ts',
                                                      'event_type',
                                                      'id as zone_id',
                                                      'distance',
                                                      'timezone') \
            .withColumn('row_number', F.row_number().over(window_for_result_distance)) \
            .withColumn('event_date',
                        F.when(F.col('event_type') == 'message', F.col('message_ts')).otherwise(F.col('event_ts'))) \
            .filter(F.col('row_number') == 1)

        window_for_registration_date = Window().partitionBy('message_from').orderBy('message_ts')

        filtred_data = result_distance_w_rn.withColumn('reaction',
                                                       F.when(F.col('event_type') == 'reaction', 1).otherwise(0)) \
            .withColumn('subscription', F.when(F.col('event_type') == 'subscription', 1).otherwise(0)) \
            .withColumn('message', F.when(F.col('event_type') == 'message', 1).otherwise(0)) \
            .withColumn('week', F.weekofyear("event_date")) \
            .withColumn('month', F.month("event_date")) \
            .withColumn("rn_for_user", F.row_number().over(window_for_registration_date)) \
            .withColumn("registration", F.when(F.col("rn_for_user") == 1, 1).otherwise(0))

        window_for_week = Window().partitionBy(['zone_id', 'week']).orderBy('event_date')

        window_for_month = Window().partitionBy(['zone_id', 'month']).orderBy('event_date')

        result = filtred_data.withColumn("week_message", F.sum(F.col("message")).over(window_for_week)) \
            .withColumn("week_reaction", F.sum(F.col("reaction")).over(window_for_week)) \
            .withColumn("week_subscription", F.sum(F.col("subscription")).over(window_for_week)) \
            .withColumn("week_user", F.sum(F.col('registration')).over(window_for_week)) \
            .withColumn("month_message", F.sum(F.col("message")).over(window_for_month)) \
            .withColumn("month_reaction", F.sum(F.col("reaction")).over(window_for_month)) \
            .withColumn("month_subscription", F.sum(F.col("subscription")).over(window_for_month)) \
            .withColumn("month_user", F.sum(F.col('registration')).over(window_for_month)) \
            .drop('user_id') \
            .drop('event')

        result = result.select('month',
                               'week',
                               'zone_id',
                               'week_message',
                               'week_reaction',
                               'week_subscription',
                               'week_user',
                               'month_message',
                               'month_reaction',
                               'month_subscription',
                               'month_user')
        return result

    data = get_user_city_info(df_path)

    writer = partition_writer(data)
#запись файла в директорию data_marts. В output_base_path передается название витрины
    writer.save(f'/user/iamegoor/data/data_marts/{output_base_path}')


if __name__ == "__main__":
    main()