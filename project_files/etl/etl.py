import pymongo
from sqlalchemy import create_engine
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import re
import pandas as pd
import logging

s  = SentimentIntensityAnalyzer()

# Establish a connection to the MongoDB server
client = pymongo.MongoClient(host="mongodb", port=27017)

# Select the database you want to use withing the MongoDB server
db = client.twitter
docs = db.tweets.find()

def clean_tweets(tweet):
    """clean the data with queries"""
    tweet = re.sub('@[A-Za-z0-9]+', '', tweet)  #removes @mentions
    tweet = re.sub('#', '', tweet) #removes hashtag symbol
    tweet = re.sub('RT\s', '', tweet) #removes RT to announce retweet
    tweet = re.sub('https?:\/\/\S+', '', tweet) #removes most URLs
    
    return tweet

def extract(docs):
    """ Read data from MongoDB, clean them and return the entries"""
    clean_entries = []
    for doc in docs:
        text = doc['text']
        entry = clean_tweets(text)
        clean_entries.append(entry)
    return clean_entries

pg = create_engine('postgresql://postgres:password@postgresdb:5432/twitter', echo=True)

pg.execute('''
    CREATE TABLE IF NOT EXISTS tweets (
    text VARCHAR(500),
    sentiment NUMERIC
);
''')

def transform(entries):
    """perform Sentiment Analysis and return entries with sentiments"""
    for entry in entries: 
        logging.critical(entry) 
        sentiment = s.polarity_scores(entry)
        score = sentiment['compound']
        query = "INSERT INTO tweets VALUES (%s, %s);"
        pg.execute(query, (entry, score))
        print(sentiment)

entries = extract(docs)

transform(entries)
df = pd.DataFrame(entries, columns=['tweet_text'])

def load(df):
    """store the results in a dataframe and in a csv file"""
    pol_scores = df['tweet_text'].apply(s.polarity_scores).apply(pd.Series)
    df=pd.concat([df, pol_scores], axis=1)
    df.to_csv("tweets_with_sentiment_analysis.csv")
    return df

load(df) 

