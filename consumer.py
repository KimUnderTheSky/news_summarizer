from kafka import KafkaConsumer

BROKER_SERVERS = ["localhost:9092"]
TOPIC_NAME = "sample_topic"

consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers = BROKER_SERVERS)

print("wait...")


# Consumer는 파이썬의 Generator로 구현되어 있다.
for message in consumer:
    print(message)

print("Done")