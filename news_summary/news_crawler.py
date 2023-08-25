
from mysettings import BROKER_URL
from bs4 import BeautifulSoup
from apscheduler.schedulers.blocking import BlockingScheduler
import os, json, sqlite3, requests
from kafka import KafkaProducer
# 뉴스 정보 토픽 설정
TOPIC_NAME = "news"

# 브로커 설정
BROKERS = [BROKER_URL]
# SQLite 데이터베이스에 연결
db_directory = "/home/ubuntu/working/kafka-examples/news_summary"
db_file = "news_database.db"
db_path = os.path.join(db_directory, db_file)



# 크롤링할 웹페이지의 URL 설정
news_url = 'https://kr.investing.com/news/latest-news'
base_url = "https://kr.investing.com"

def crawl_news():
    main_response = requests.get(news_url)
    main_html_content = main_response.text
    main_soup = BeautifulSoup(main_html_content, 'html.parser')
    # 클래스가 "content"인 <div> 태그 찾기
    content_divs = main_soup.find_all('div', class_='textDiv')

    # 각 뉴스 기사 링크 가져오기
    # 각 뉴스 기사 제목 가져오기
    link_list = []
    title_list = []
    for content_div in content_divs:
        a_tags = content_div.find_all('a', class_='title', recursive=False)
        for a_tag in a_tags:
            url = base_url + a_tag['href']
            title = a_tag['title']
            link_list.append(url)
            title_list.append(title)
            
    
    # 각 뉴스 기사마다 텍스트 가져오기
    news_texts = []    
    for link in link_list:
        sub_response = requests.get(link)
        sub_html_content = sub_response.text
        sub_soup = BeautifulSoup(sub_html_content, 'html.parser')

        text_divs = sub_soup.find('div', class_= "WYSIWYG articlePage")
        p_tags = text_divs.find_all('p', recursive=False)
        p_texts = [p_tag.get_text() for p_tag in p_tags]
        # 리스트의 요소들을 하나의 문장으로 합치기
        combined_sentence = ' '.join(p_texts)   
        news_texts.append(combined_sentence)
    return title_list, link_list, news_texts

# DB에 넣는 쿼리
def insert_data(conn, title, url, text):
    cursor = conn.cursor()
    
    # 해당 URL이 이미 데이터베이스에 존재하는지 확인
    select_query = "SELECT url FROM news WHERE url = ?;"
    cursor.execute(select_query, (url,))
    existing_url = cursor.fetchone()

    if existing_url is None:
        if len(text) > 20 and len(title) > 5:  # text가 20자 이상인 경우에만 데이터 삽입
            insert_query = "INSERT INTO news (title, url, text) VALUES (?, ?);"
            cursor.execute(insert_query, (title, url, text))
            conn.commit()

            # # Kafka로 데이터 전송
            row_data = {
                "TITLE": title,
                "URL": url,
                "TEXT": text
            }
            # dumps -> 딕셔너리를 Json 형식의 바이너리화 된 문자열로 바꿔준다.
            row_json = json.dumps(row_data).encode("utf-8")

            # 데이터 스트리밍
            producer.send(TOPIC_NAME, row_json)
            producer.flush()
            # print(row_data)
            return # 테스팅 일단 한개만 flush
            
        else:
            print(f"Text for URL '{url}' is too short (<= 20 characters). Skipping insertion.")
    else:
        print(f"URL '{url}' already exists in the database. Skipping insertion.")

# 크롤링 된 데이터 DB에 저장 
def db_save(conn, title_list, link_list, news_texts):
    # 데이터 저장
    for title, url, text in zip(title_list, link_list, news_texts):
        insert_data(conn, title, url, text)

# 크롤링, DB적재
def crawl_and_insert():
    conn = sqlite3.connect(db_path)
    title_list, link_list, news_texts = crawl_news()
    db_save(conn, title_list, link_list, news_texts)
    conn.close()

# crawl_and_insert()
# 스케줄링 설정
scheduler = BlockingScheduler()

# 매 시간마다 crawl_and_insert 함수 실행
scheduler.add_job(crawl_and_insert, 'interval', minutes=1)
if __name__ == "__main__":

    producer = KafkaProducer(bootstrap_servers=BROKERS)

    print("스케줄링을 시작합니다. Ctrl+C를 눌러 종료하세요.")
    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("스케줄링을 종료합니다.")