# 데이터엔지니어링 프로젝트
gpt 기반 해외 뉴스 해설 및 요약 알리미

---

## 서비스 로직

1. 1시간 주기로 크롤링
    1. 뉴스 제목, 뉴스 url, 뉴스 기사 내용 크롤링 
2. 크롤링 후, 이미 slack에 보낸 기사인지 확인.
    1. 새로들어온 기사의 url이 url_set 집합에 있는지 체크
    2. 없다면 url을 url_set에 추가 후, kafka 스트리밍
3. summarizer에서 gpt 기반 뉴스 요약
    1. prompt engineering(few shot prompting) 활용 
    2. 기존 뉴스기사 해설 내용을 바탕으로 gpt 학습
4. 슬랙 전송 (카카오톡 연동 가능)

---

## 시스템 구조

1. 뉴스 기사 크롤링(BeautifulSoup,apscheduler)
2. kafka streaming(kafka producer, kafka consumer)
2. chat gpt 기반으로 뉴스 요약 정보 생성(openai)
3. 슬랙, kakao talk 전송(request, webhook)
![image](https://github.com/KimUnderTheSky/news_summarizer/assets/96776691/822f9693-9917-44a6-b02d-4d57eb55a69d)