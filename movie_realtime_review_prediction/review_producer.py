from kafka import KafkaProducer
import pandas as pd
import urllib.request
import time
import json
from mysettings import BROKER_URL
# 결제 정보 토픽 설정
TOPIC_NAME = "naver_reviews"

# 브로커 설정
BROKERS = [BROKER_URL]

producer = KafkaProducer(bootstrap_servers = BROKERS)

urllib.request.urlretrieve("https://raw.githubusercontent.com/e9t/nsmc/master/ratings_test.txt", filename="ratings_test.txt")
df_test  = pd.read_table('ratings_test.txt')

for sentence in df_test['document']:

    msg = {
        "sentence": sentence
    }

    producer.send(TOPIC_NAME, json.dumps(msg).encode("utf-8"))
    print(msg)
    time.sleep(1)

    producer.flush()