from kafka import KafkaConsumer
import json
from mysettings import BROKER_URL, WEBHOOK_URL

FRAUD_TOPIC = "fraud_payments"
BROKERS = [BROKER_URL]

def send_slack(msg):
    import requests
    
    payloads = {
        "channel": "#기타자료",
        "username": "이름을 바꿔주세요",
        "text": msg
    }

    requests.post(WEBHOOK_URL, json.dumps(payloads))

if __name__ == "__main__":
    consumer = KafkaConsumer(FRAUD_TOPIC, bootstrap_servers=BROKERS)

    for message in consumer:
        msg= json.loads(message.value.decode())

        payment_type = msg["PAYMENT_TYPE"]
        payment_date = msg["DATE"]
        payment_time = msg["TIME"]
        amount = msg["AMOUNT"]
        to = msg["TO"]

        fraud_msg = f"[이상 거래] : {payment_type} {payment_date} {payment_time} << {to} - {amount} >>"
        # send_slack(fraud_msg)
        print(fraud_msg)