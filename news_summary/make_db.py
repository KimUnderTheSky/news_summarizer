
import sqlite3
import os
# SQLite 데이터베이스 연결 및 테이블 생성
def setup_database():
    # 데이터베이스 파일 경로
    db_directory = "/home/ubuntu/working/kafka-examples/news_summary"
    db_file = "news_database.db"
    db_path = os.path.join(db_directory, db_file)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS news (
            id INTEGER PRIMARY KEY,
            title TEXT,
            url TEXT,
            text TEXT
        )
    ''')
    conn.commit()
    conn.close()

setup_database()