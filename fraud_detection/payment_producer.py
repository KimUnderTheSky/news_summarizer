from kafka import KafkaProducer
import datetime
import pytz
import time
import json
import random
from mysettings import BROKER_URL
# 결제 정보 토픽 설정
TOPIC_NAME = "payments"

# 브로커 설정
BROKERS = [BROKER_URL]

# 리눅스 시간이 아닌, 서울 시간으로 시간 데이터를 생성하기 위한 함수
def get_seoul_datetime():
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())
    kst_now = utc_now.astimezone(pytz.timezone("Asia/Seoul"))

    d = kst_now.strftime("%m/%d/%y")
    t = kst_now.strftime("%H:%M:%S")

    return d, t

# 임시 결제 정보 데이터 발생기 - 랜덤하게 결제 정보를 만듦
def generate_payment_data():
    # 데이터 소스로부터 데이터를 끌어오는 기능을 구현해 주시면 됩니다.(크롤링)
    # 크롤링이나 다른 데이터 레이크에서 데이터를 끌어올 수도 있다.
    
    # 결제 방식
    payment_type = random.choice(["롯데카드","삼성카드","현대카드","비트코인","이더리움"])
    amount = random.randint(1000, 10000000)
    
    # 결제자
    to = random.choice(["me","mom","dad","stranger"])

    return payment_type, amount, to

if __name__ == "__main__":

    producer = KafkaProducer(bootstrap_servers=BROKERS)

    # 데이터 발생 및 스트리밍
    while True:
        # 현재 시간데이터를 얻기
        d, t = get_seoul_datetime()

        # 랜덤하게 만들어진 결제 정보 얻기
        payment_type, amount, to = generate_payment_data()

        # 스트리밍할 데이터를 조립 (일반적으로는 json이 제일 간편.)
        row_data = {
            "DATE": d,
            "TIME": t,
            "PAYMENT_TYPE": payment_type,
            "AMOUNT": amount,
            "TO": to
        }
        # dumps -> 딕셔너리를 Json 형식의 바이너리화 된 문자열로 바꿔준다.
        row_json = json.dumps(row_data).encode("utf-8")

        # 데이터 스트리밍
        producer.send(TOPIC_NAME, row_json)
        producer.flush()
        print(row_data)
        time.sleep(1)



