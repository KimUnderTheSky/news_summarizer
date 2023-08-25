from kafka import KafkaConsumer

import json

BROKER_SERVERS = ["localhost:9092"]
TOPIC_NAME = "sample_topic"

consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers = BROKER_SERVERS)

for message in consumer:
    # message에서 value를 추출하면 Producer가 보낸 값
    row = json.loads(message.value.decode())
    # 받아오는 데이터를 가지고 머신러닝 파이프라인을 태운다.
    # - 실시간 예측
    # - 실시간 군집
    # - 실시간 추천 등...
    # - 실시간 집계 등등...
