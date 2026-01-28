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

def create_spark_job(file: str, job_name: str):
    return SparkSubmitOperator(
        task_id=f'run-{job_name}',
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

with DAG(
    dag_id='plane_flights_data_pipeline',
    default_args=default_args,
    description='Run airplane flights data pipeline',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    root_job = create_root_job()
    
    clean_batch_data_job_config = ('clean_data', 'clean_data')
    clean_batch_data_job = create_spark_job(clean_batch_data_job_config[0], clean_batch_data_job_config[1])

    transform_batch_data_jobs_config = [
        ('b1', 'test'),
    ]
    transform_batch_data_jobs = [create_spark_job(file, job_name) for file, job_name in transform_batch_data_jobs_config]

    root_job >> clean_batch_data_job >> transform_batch_data_jobs