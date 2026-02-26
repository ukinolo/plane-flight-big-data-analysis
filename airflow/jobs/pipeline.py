import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'retries': 0,
}

spark_env_vars = {
    "JAVA_HOME": "/usr/lib/jvm/temurin-11-jdk-amd64",
    "CORE_CONF_fs_defaultFS": "hdfs://namenode:9000",
    "CORE_CONF_hadoop_http_staticuser_user": "root",
    "CORE_CONF_hadoop_proxyuser_hue_hosts": "*",
    "CORE_CONF_hadoop_proxyuser_hue_groups": "*",
    "HDFS_CONF_dfs_webhdfs_enabled": "true",
    "HDFS_CONF_dfs_permissions_enabled": "false",
    "PYSPARK_PYTHON": "python3",
    "MONGO_URI": "mongodb://mongodb:27017",
    "HADOOP_CONF_DIR": "/opt/hadoop/conf",
}

def create_root_job():
    return EmptyOperator(task_id='run-plane-flights-data-pipeline')

def create_spark_job(file: str):
    return SparkSubmitOperator(
        task_id=f'run-{file}',
        application=f'/opt/spark/scripts/{file}.py',
        conn_id="spark_custom",
        conf={
            "spark.executor.memory": "1g",
            "spark.driver.memory": "1g",
            "spark.executor.cores": "1",
            "spark.sql.shuffle.partitions": "200"
        },
        env_vars=spark_env_vars,
        packages="org.mongodb.spark:mongo-spark-connector_2.12:10.5.0",
        verbose=True,
    )

def create_spark_stream_job(file: str):
    return SparkSubmitOperator(
        task_id=f'run-{file}',
        application=f'/opt/spark/scripts/{file}.py',
        conn_id="spark_custom",
        conf={
            "spark.executor.memory": "3g",
            "spark.driver.memory": "3g",
            "spark.executor.cores": "2",
            # "spark.executor.instances": "2",
            "spark.cores.max": "16",
        },
        env_vars=spark_env_vars,
        packages=",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1",
            "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0",
        ]),
        verbose=True,
    )

with DAG(
    dag_id='plane_flights_data_pipeline',
    default_args=default_args,
    description='Run airplane flights data pipeline',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    root_job = create_root_job()
    
    clean_batch_data_job = create_spark_job('clean_data')

    transform_batch_data_jobs_config = [
        'b1',
        'b2',
        'b3',
        'b4',
        'b5',
        'b6',
        'b7',
        'b8',
        'b9',
        'b10',
    ]
    transform_batch_data_jobs = [create_spark_job(file) for file in transform_batch_data_jobs_config]

    root_job >> clean_batch_data_job >> transform_batch_data_jobs

with DAG(
    dag_id='Batch_part_clean',
    default_args=default_args,
    description='Run airplane flights data cleaning',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    root_job = create_root_job()
    
    clean_batch_data_job = create_spark_job('clean_data')

    root_job >> clean_batch_data_job

with DAG(
    dag_id='Batch_part_1',
    default_args=default_args,
    description='Run airplane flights data pipeline 1',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    root_job = create_root_job()

    transform_batch_data_jobs_config = [
        'b1',
        'b2',
        'b3',
    ]
    transform_batch_data_jobs = [create_spark_job(file) for file in transform_batch_data_jobs_config]

    root_job >> transform_batch_data_jobs

with DAG(
    dag_id='Batch_part_2',
    default_args=default_args,
    description='Run airplane flights data pipeline 2',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    root_job = create_root_job()

    transform_batch_data_jobs_config = [
        'b4',
        'b5',
        'b6',
        'b7',
    ]
    transform_batch_data_jobs = [create_spark_job(file) for file in transform_batch_data_jobs_config]

    root_job >> transform_batch_data_jobs

with DAG(
    dag_id='Batch_part_3',
    default_args=default_args,
    description='Run airplane flights data pipeline 3',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    root_job = create_root_job()

    transform_batch_data_jobs_config = [
        'b8',
        'b9',
        'b10',
    ]
    transform_batch_data_jobs = [create_spark_job(file) for file in transform_batch_data_jobs_config]

    root_job >> transform_batch_data_jobs

with DAG(
    dag_id='plane_flights_stream_jobs_start',
    default_args=default_args,
    description='Run airplane streaming jobs',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    root_job = create_root_job()
    
    transform_stream_data_jobs_config = [
        'clean_stream_data',
        's1',
        # 's2',
        # 's3',
        # 's4',
        # 's5',
    ]
    transform_stream_data_jobs = [create_spark_stream_job(file) for file in transform_stream_data_jobs_config]

    root_job >> transform_stream_data_jobs