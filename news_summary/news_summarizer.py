from kafka import KafkaConsumer, KafkaProducer
import json
import openai
from mysettings import BROKER_URL, WEBHOOK_URL, OPENAPI_KEY
NEWS_TOPIC = "news"
BROKERS = [BROKER_URL]


openai.api_key = OPENAPI_KEY

def send_slack(msg):
    import requests
    
    payloads = {
        "channel": "#개인",
        "username": "금융뉴스알리미",
        "text": msg
    }

    requests.post(WEBHOOK_URL, json.dumps(payloads))


# msg.text로 gpt로 답변 받아오기
# 뉴스 기사에서 전문 용어를 설명해주는 것.
BASE_PROMPT_TC = [
        {"role": "system", "content": "이 시스템은 일반인들을 대상으로 금융, 경제 뉴스 기사를 해석하여 보여주는 시스템이다."},
        {"role": "user", "content": """
경상수지가 19개월째 흑자 행진을 이어갔다. 지난해 11월 경상수지는 71억6천만달러 흑자로 집계됐다.
한국은행은 11일 ‘2021년 11월 국제수지(잠정)’ 자료를 통해 지난해 11월 경상수지는 71억6천만달러 흑자를 기록했다고 밝혔다. 지난해 5월 이후 19개월 연속 흑자 행진이다.
경상수지는 외국과 물건(재화)이나 서비스(용역)를 팔고 산 결과를 말한다. 부문별로 살펴 보면 수출과 수입의 차이를 보여주는 상품수지가 59억5천만달러 흑자를 나타냈다. 수출은 596억5천만달러, 수입은 537억달러를 각각 기록했다. 다만 상품수지 흑자 규모는 전년(99억5천만달러)에 비해서는 40억달러 축소됐다. 수출 호조세가 이어졌으나 원자재값 상승으로 수입 가격도 오르면서 흑자 규모가 작아진 것이다.
서비스수지는 1억4천만달러 적자를 나타냈다. 그러나 세계적인 공급망 차질로 우리나라 해운과 항송 운송 수입도 늘어나면서 적자 규모는 전년(9억8천만달러) 대비 줄었다.
임금·배당·이자 흐름을 반영하는 본원소득수지는 14억9천만달러 흑자다. 전년 같은 기간(4억8천만달러)과 비교하면 흑자 규모가 10억1천만달러 늘었다. 내국인들의 해외 직접투자 등이 증가하면서 배당소득수지가 전년 4억3천만달러 적자에서 이번에 6억7천만달러 흑자로 전환했다.
앞서 올해 1월 1일 산업통상자원부 발표에 따르면, 지난해 12월 무역수지는 5억9천만달러 적자로 집계됐다. 사상 최고 수출 실적에도 수입 원자재 가격 상승 폭이 너무 컸기 때문이다. 이로 인해 한은의 경상수지 내 상품수지도 작년 12월 통계가 적자로 돌아설 수 있다는 우려가 나오고 있다. 무역수지와 상품수지는 해외 생산 수출, 수입 보험 및 운송비 등 구성 항목이 다소 다르다.
이성호 한은 금융통계부장은 “지난해 12월 상품수지 전망에 대해서는 아직 해외 생산 수출 등이 집계 전이라 상황을 더 지켜봐야 할 것”이라고 말했다.
        """},
        {"role": "assistant", "content": """
경상 수지가 19개월 연속 흑자행진이다. 부문별로 살펴보면 상품수지는 흑자이지만 작년대비 흑자 규모가 줄었다. 수출은 호조였으나 원자재 가격이 오르면서 수입비용이 증가했기 때문이다. 
서비스 수지는 적자였지만 작년대비 적자 규모가 줄었다. 세계적인 공급망 차질로 우리나라 해운과 항송 운송 수입이 늘었기 때문이다. (해운과 항송은 운송으로 서비스 수지에 해당)
소득수지는 흑자이고 작년대비 흑자규모도 늘었다. 내국인의 해외 투자등이 증가하며 배당소득수지가 늘었기 때문이다.
무역수지는 적자이다. 이 역시 원자재 가격이 많이 상승했기 때문이다. 대한민국은 제조업 기반의 국가이기 때문에 전체 경상수지 흑자 규모 71억 6천만 달러 대비 상품수지 흑자 규모가 59억 5천만 달러이다. 그만큼 한국이 제조업에 많은 의존을 하고 있다는 것을 알 수 있다. 근데 최근에 한류 열풍이 전세계에 불고 있는데 서비스 수지에 어느정도 영향을 미치고 있는지 궁금했다. 찾아보니까 문화예술저작권 무역수지가 2020년에 사상 첫 흑자를 기록했다고 한다. 그리고 세계적인 공급망 차질로 우리나라의 해운, 항송 운송 수입이 늘어났다는 점은 흥미로웠다. 공급망 차질로 인해 운송업계에서 수요가 더 생겼고 수요는 수입으로 이어졌다.
미국이 금리를 계속 인상하는 정책을 펼치다. 점차 달러의 가치가 높아져서 한국의 상품 가격경쟁력이 높아지고, 미국에 수출이 늘어나고, 대미무역수지 흑자규모가 더 커질 수 있음을 예상해볼 수 있다.
        """}
]






# 슬랙으로 send
if __name__ == "__main__":
    consumer = KafkaConsumer(NEWS_TOPIC, bootstrap_servers = BROKERS)
    producer = KafkaProducer(bootstrap_servers=BROKERS)

    for message in consumer:
        # loads: 문자열 형태의 json 데이터를 딕셔너리 형태로 변환
        msg = json.loads(message.value.decode()) 
        
        text = msg["TEXT"]
        BASE_PROMPT_TC.append({"role": "user", "content": text})
        completion = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=BASE_PROMPT_TC)
        
        title = msg["TITLE"]
        url = msg["URL"]
        info = [completion.choices[0].message.content]
        msg = f"[기사제목] : {title}\\n[기사요약] : {info}\\n[기사링크]: {url}"
        print(msg)
        send_slack(msg)