# 데이터엔지니어링 프로젝트
gpt 기반 해외 뉴스 요약 알리미

---

## 서비스 로직

1. 1시간 주기로 크롤링 후 db에 적재(url이 없는 기사만 sqlite에 저장)
    1. 크롤링 후 DB에 있는 데이터인지 검증 후 뿌림
2. db 데이터 불러와서 kafka producer로 데이터 발생 후summarizer로 이동
3. summarizer에서 gpt 기반 뉴스 요약 - prompt engineering : few shot prompting
    1. 금융, 경제 지식 수준
    2. 무슨 사건인지 일어나는지
    3. 금융시장, 증시에 어떤 영향을 미칠지
    4. 금융용어 학습
    5. 대응책
4. 슬랙, kakao talk 전송

---

## 시스템 구조

1. 뉴스기사 크롤링 후, kafka기반 스트리밍
2. chat gpt 기반으로 뉴스 요약 정보 생성
3. 슬랙, kakao talk 전송
