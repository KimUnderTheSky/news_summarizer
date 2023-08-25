from kafka import KafkaProducer

import csv
import json
import time

BROKER_SERVERS = ["localhost:9092"]
TOPIC_NAME = "sample_topic"


producer = KafkaProducer(bootstrap_servers = BROKER_SERVERS)

# CSV 파일 열기
with open("/home/ubuntu/working/spark-examples/data/trips/yellow_tripdata_2021-04.csv") as f:
    reader = csv.reader(f)

    # reader에서 한줄 씩 불러오기
    for row in reader:
        time.sleep(0.5)
        producer.send(TOPIC_NAME, json.dumps(row).encode("utf-8"))
        print(json.dumps(row).encode("utf-8"))