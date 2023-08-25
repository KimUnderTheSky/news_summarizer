from kafka import KafkaConsumer, KafkaProducer
import json

NEWS_TOPIC = "news"
BROKERS = ["ec2-54-180-4-129.ap-northeast-2.compute.amazonaws.com:9092"]

if __name__ == "__main__":
    consumer = KafkaConsumer(NEWS_TOPIC, bootstrap_servers = BROKERS)
    producer = KafkaProducer(bootstrap_servers=BROKERS)

    for message in consumer:
        # loads: 문자열 형태의 json 데이터를 딕셔너리 형태로 변환
        msg = json.loads(message.value.decode()) 
        print(msg)
        