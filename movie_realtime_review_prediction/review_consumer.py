from kafka import KafkaConsumer
import json
# pickle로 저장된 모델 파일을 불러오기 위해 import
from joblib import load
from mysettings import BROKER_URL
# 결제 정보 토픽 설정
TOPIC_NAME = "naver_reviews"

# 브로커 설정
BROKERS = [BROKER_URL]

consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers = BROKERS)

#tfidfvectorizer, model 파일 불러오기
tfidf_vectorizer = load("/home/ubuntu/working/kafka-examples/tfidf_vectorizer.pkl")
model = load("/home/ubuntu/working/kafka-examples/korean_model.pkl")

print("Wait...")
for msg in consumer:
    sentence = json.loads(msg.value.decode())["sentence"]
    #벡터화
    test_vector = tfidf_vectorizer.transform([sentence])
    prediction = model.predict(test_vector)[0]


print("Done....")