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
        {"role": "system", "content": "이 시스템은 일반인들을 대상으로 금융, 경제 뉴스 기사를 한글로 해설하여 보여주는 시스템이다."},
        {"role": "user", "content": """
Korea's current account surplus continued for the 19th consecutive month in November, reaching a surplus of $716 million. The Bank of Korea announced on the 11th that according to the preliminary data for the "International Balance of Payments for November 2021," the current account surplus for November last year was $716 million. This marks the 19th month of continuous surplus since May of the previous year.

The current account refers to the result of selling and buying goods and services with foreign countries. When examining the different sectors, the balance of goods, which shows the difference between exports and imports, recorded a surplus of $595 million. Exports were $59.65 billion, while imports were $53.7 billion, respectively. However, the size of the goods balance surplus decreased by $4 billion compared to the previous year ($9.95 billion). While export performance remained strong, the surplus decreased due to the rise in import prices caused by increases in raw material costs.

The balance of services showed a deficit of $140 million. However, due to disruptions in global supply chains, there was an increase in imports related to shipping and transportation, resulting in a smaller deficit compared to the previous year ($980 million).

Primary income, which reflects the flow of wages, dividends, and interest, recorded a surplus of $1.49 billion. This is an increase of $1.01 billion compared to the same period in the previous year ($480 million). The surplus in primary income increased as overseas direct investments by domestic residents rose, leading to a transition from a deficit of $430 million in dividends to a surplus of $670 million.

In contrast, according to data released by the Ministry of Trade, Industry and Energy on January 1st of this year, the trade balance recorded a deficit of $590 million in December of the previous year. This was due to the excessive increase in the price of imported raw materials despite achieving record-high export figures. As a result, concerns are arising that the goods balance within the current account, as well as the overall trade balance, might turn into a deficit in December's statistics. The trade balance and the goods balance have slightly different components, including overseas production exports, import insurance, and transportation costs.

Lee Sung-ho, the director of the Bank of Korea's Financial Statistics Department, stated, "Regarding the outlook for the goods balance in December of last year, we need to monitor the situation further as overseas production exports and other factors are yet to be fully accounted for."

Please note that this translation is provided for informational purposes and may require further review by a professional translator for official or critical use.
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
        try:
            # loads: 문자열 형태의 json 데이터를 딕셔너리 형태로 변환
            msg = json.loads(message.value.decode()) 


            text = msg["TEXT"]
            current_messages = BASE_PROMPT_TC + [{"role": "user", "content": text}]

            # BASE_PROMPT_TC.append({"role": "user", "content": text})

            completion = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=current_messages,
                max_tokens=4096
                )

            # BASE_PROMPT_TC.pop()
            print("token구조: \n",BASE_PROMPT_TC)
            title = msg["TITLE"]
            url = msg["URL"]
            info = [completion.choices[0].message.content]
            msg = f"[기사제목] : {title} \n[기사요약] : {info} \n[기사링크]: {url}"
            # print(msg)
            send_slack(msg)
        except openai.error.InvalidRequestError as e:
            print("에러 발생:", e)
            continue