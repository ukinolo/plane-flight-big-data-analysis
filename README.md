# Big Data Engineering - Flight Data Analysis Platform

The purpose of this project was to build a complete data engineering platform that processes large volumes of aviation data in both historical and real-time contexts. It represents a practical exploration of how modern big data tools can be used to construct scalable analytics systems.

## Architecture

The system is composed of two main processing layers with different data sources:
- Batch layer: processes historical flight data and produces long-term operational insights.
- Streaming layer: processes live aircraft telemetry and produces real-time analytics.

### Diagram
![Architecture](architecture.png "Architecture")

### Overview

Both batch and real-time pipelines follow a layered data architecture consisting of:
 - Raw zone
 - Curated zone
 - Transformed zone

Batch data ingestion is performed using **Apache NiFi** which load data into the raw data zone stored on **Apache Hadoop**. **Apache Spark** jobs are used to clean data, storing it in the curated zone on **Apache Hadoop**. Additional **Apache Spark** jobs are used to do analytical transformation which are stored in the **MongoDB**.

Live aircraft telemetry is ingested via a custom **Python producer** and stored into **Apache Kafka**.
**Spark Structured Streaming** is used for:
 - curating raw data and writing it into the **Apache Kafka**
 - transforming curated data and writing it into **MongoDB**.

**Metabase** is used to query transformed data from **MongoDB** and present analytical dashboards.

Batch data source: [link](https://www.kaggle.com/datasets/robikscube/flight-delay-dataset-20182022)

Real-time data source [link](https://openskynetwork.github.io/opensky-api/rest.html)

## Commands for running spark jobs directly inside container

 - connect to the spark master container: `docker exec -it spark-master bash`

 - once in the bash of spark master container run: `/spark/bin/spark-submit --master spark://spark-master:7077 /home/scripts/<script_name>`
    - example `/spark/bin/spark-submit --master spark://spark-master:7077 /home/scripts/clean_data.py`
    - example `/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.mongodb.spark:mongo-spark-connector_2.12:10.5.0 /home/scripts/b1.py`
    - example `/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /home/scripts/clean_stream_data.py`
    - example `/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.mongodb.spark:mongo-spark-connector_2.12:10.5.0 --executor-cores 1 /home/scripts/clean_stream_data.py`

## Commands for handling kafka

- read all the messages from the topic:
``` bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic stream-flights-raw \
  --from-beginning \
  --property print.key=true \
  --property print.value=true
```

- delete kafka topic
```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic stream-flights-raw

```

- message count
```bash
kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic stream-flights-raw \
  --time -1
```