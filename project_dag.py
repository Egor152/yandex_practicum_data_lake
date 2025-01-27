from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
                                'owner': 'airflow',
                                'start_date':datetime(2022, 5, 31),
                                }

dag_spark = DAG(
                        dag_id = "project_dag",
                        default_args=default_args,
                        schedule_interval=None,
                        )

start_task = DummyOperator(
    task_id='start_task',
    dag=dag_spark
)


# объявляем задачу на запись данных для первой витрины с помощью SparkSubmitOperator
first_mart = SparkSubmitOperator(
                        task_id='first_mart',
                        dag=dag_spark,
                        application ='/lessons/first_script.py',
                        conn_id= 'yarn_spark',
                        application_args = ["/user/master/data/geo/events", "first_mart"],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )
# объявляем на запись данных для второй витрины задачу с помощью SparkSubmitOperator
second_mart = SparkSubmitOperator(
                        task_id='second_mart',
                        dag=dag_spark,
                        application ='/lessons/second_script_for_airflow.py',
                        conn_id= 'yarn_spark',
                        application_args = ["/user/master/data/geo/events", "second_mart"],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )

# объявляем задачу на запись данных для третьей витрины с помощью SparkSubmitOperator
third_mart = SparkSubmitOperator(
                        task_id='third_mart',
                        dag=dag_spark,
                        application ='/lessons/third_script_for_airflow.py',
                        conn_id= 'yarn_spark',
                        application_args = ["/user/master/data/geo/events", "third_mart"],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )


end_task = DummyOperator(
    task_id='end_task',
    dag=dag_spark
)


start_task >> first_mart >> second_mart >> third_mart >> end_task