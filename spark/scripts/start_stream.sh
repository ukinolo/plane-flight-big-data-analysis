#!/bin/bash

cleanup() {
    echo "Stopping all Spark jobs..."
    pkill -P $$
    exit 1
}

trap cleanup SIGINT SIGTERM

/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.mongodb.spark:mongo-spark-connector_2.12:10.5.0 --total-executor-cores 5 /home/scripts/clean_stream_data.py &
spark_submit_pids+=($!)

/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.mongodb.spark:mongo-spark-connector_2.12:10.5.0 --total-executor-cores 5 /home/scripts/s1.py &
spark_submit_pids+=($!)

/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.mongodb.spark:mongo-spark-connector_2.12:10.5.0 --total-executor-cores 5 /home/scripts/s2.py &
spark_submit_pids+=($!)

/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.mongodb.spark:mongo-spark-connector_2.12:10.5.0 --total-executor-cores 5 /home/scripts/s3.py &
spark_submit_pids+=($!)

/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.mongodb.spark:mongo-spark-connector_2.12:10.5.0 --total-executor-cores 5 /home/scripts/s4.py &
spark_submit_pids+=($!)

/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.mongodb.spark:mongo-spark-connector_2.12:10.5.0 --total-executor-cores 5 /home/scripts/s5.py &
spark_submit_pids+=($!)

wait