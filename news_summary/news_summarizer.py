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
        {"role": "system", "content": "이 시스템은 일반인들을 대상으로 뉴스 사건이 경제 및 금융시장에 미칠 영향을 한글로 해설하여 보여주는 시스템이다."},
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

For 19 consecutive months, South Korea has maintained a surplus streak. Looking at the different sectors, the goods trade balance remains in surplus, although the surplus has decreased compared to the previous year. While exports have been strong, the increase in raw material prices has led to higher import costs.

The services trade balance was in deficit, but the deficit has reduced compared to the previous year. This is attributed to an increase in imports of maritime and shipping services due to global supply chain disruptions.

The income balance is in surplus, and the surplus has expanded compared to the previous year. This growth can be attributed to increased overseas investments by domestic residents, leading to higher dividend income.

However, the overall trade balance is in deficit. This is also influenced by the significant rise in raw material prices. As South Korea relies heavily on manufacturing, the surplus in goods trade balance, which stands at $595 million against the total current account surplus of $7.16 billion, underscores the country's manufacturing dependence.

Interestingly, amidst the global popularity of the Korean Wave, the trade surplus in cultural and artistic copyright exports recorded its first surplus in history in 2020. Moreover, the disruption in global supply chains led to increased imports in maritime and shipping services for South Korea, which in turn created demand and subsequently increased imports.

The United States continues its policy of raising interest rates, leading to a gradual increase in the value of the dollar. This could enhance Korea's price competitiveness for goods, potentially boosting exports to the U.S. and widening the trade surplus in the U.S.-Korea trade relationship.
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
                max_tokens=1700
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