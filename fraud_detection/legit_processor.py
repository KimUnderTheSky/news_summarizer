# 정상 데이터를 처리하기 위한 컨슈머
from kafka import KafkaConsumer
import json
from mysettings import BROKER_URL
LEGIT_TOPIC = "legit_payments"
BROKERS = [BROKER_URL]

if __name__ == "__main__":
    consumer= KafkaConsumer(LEGIT_TOPIC, bootstrap_servers=BROKERS)

    for message in consumer:
        msg = json.loads(message.value.decode())

        payment_type = msg["PAYMENT_TYPE"]
        payment_date = msg["DATE"]
        payment_time = msg["TIME"]
        amount = msg["AMOUNT"]
        to = msg["TO"]

        print(f"[결제 정보] : {payment_type} {payment_date} {payment_time} << {to} - {amount} >>")