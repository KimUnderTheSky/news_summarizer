import pandas as pd
import numpy as np
import urllib.request
import re
from sklearn.feature_extraction.text import TfidfVectorizer
# from konlpy.tag import Okt
from sklearn.linear_model import LogisticRegression
from joblib import dump
# urllib.request.urlretrieve("https://raw.githubusercontent.com/e9t/nsmc/master/ratings_train.txt", filename="ratings_train.txt")

df_train = pd.read_table('ratings_train.txt')

# 1. 한글을 제외한 나머지 데이터를 제거 - 필요없는 숫자나 영어 등을 제거
df_train = df_train.dropna(how='any')
df_train['document'] = df_train['document'].apply(lambda s : re.sub("[^ㄱ-ㅎㅏ-ㅣ가-힣 ]", "", s))

# 2. 한글 빼고 다 제거 했기 때문에, 영어로만 있거나 특수 기호로만 있었던 리뷰는 ""만 남는다.
# print(df_train.loc[df_train['document'] == '', 'document'].value_counts())
df_train['document'].replace("", np.nan, inplace=True)
df_train = df_train.dropna(how='any')

# 전체 데이터에서 10000개만 랜덤으로 추출
df_train_toy = df_train.sample(10000, random_state=42)

# Vectorize
# okt = Okt()

# tfidf_vectorizer = TfidfVectorizer(tokenizer=okt.morphs) # TfidfVectorizer에 커스텀 토크나이저 적용
tfidf_vectorizer = TfidfVectorizer()
tfidf_vectorizer.fit(df_train_toy['document'])

X_train_tfidf_vector = tfidf_vectorizer.transform(df_train_toy['document'])
y_train = df_train_toy['label']

# make model
lr_clf = LogisticRegression()
lr_clf.fit(X_train_tfidf_vector, y_train)

print(f"Accuracy : {lr_clf.score(X_train_tfidf_vector, y_train)}")

# 만들어진 모델을 저장
dump(lr_clf, "korean_model.pkl")
dump(tfidf_vectorizer, 'tfidf_vectorizer.pkl')