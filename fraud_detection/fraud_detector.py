# payment_producer에서 데이터를 consume
# 정상 데이터 -> legit_processor로 produce
# 이상 데이터 -> fraud_processor로 produce
from mysettings import BROKER_URL
from kafka import KafkaConsumer, KafkaProducer
import json

PAYMENT_TOPIC = "payments"
FRAUD_TOPIC = "fraud_payments"
LEGIT_TOPIC = "legit_payments"

BROKERS = [BROKER_URL]

# 이상 결제 기준 정의
def is_suspicious(message): # message에 JSON 데이터가 들어옵니다.
    # stranger가 결제 or 결제 금액이 500만원 이상이면 이상 결제로 결정.
    # 메시지를 feature로 받는 머신러닝 모델의 predict
    if message["TO"] == "stranger" or message["AMOUNT"] >= 5000000:
        return True
    else:
        return False


if __name__ == "__main__":
    consumer = KafkaConsumer(PAYMENT_TOPIC, bootstrap_servers = BROKERS)
    producer = KafkaProducer(bootstrap_servers=BROKERS)

    for message in consumer:
        # loads: 문자열 형태의 json 데이터를 딕셔너리 형태로 변환
        msg = json.loads(message.value.decode()) 

        if is_suspicious(msg):
            producer.send(FRAUD_TOPIC, json.dumps(msg).encode("utf-8"))
        else:
            producer.send(LEGIT_TOPIC, json.dumps(msg).encode("utf-8")) 
        producer.flush()
        print(is_suspicious(msg), msg["TO"],msg["AMOUNT"])