import mysql.connector
from sqlalchemy import create_engine
import pymysql
from password import USERNAME, PASSWORD

import pandas as pd
import datetime

import smtplib

# database connection
sqlEngine = create_engine(f'mysql+pymysql://{USERNAME}:{PASSWORD}@localhost/twitter')

# reading from 
df = pd.read_sql_table('tweets',sqlEngine)

links = []

# adding new column for article links 
for row in df.iterrows():
    links.append(row[1]['tweet'].split(' ')[-1])

df['link'] = links
df['timestamp'] = df['timestamp'].apply(lambda x: x.date())

yesterdays_date = datetime.date.today() - datetime.timedelta(1)
top_3_tweets = df[df['timestamp'] == yesterdays_date].sort_values('retweets', ascending=False).head(3)

# function to remove link from tweet 
def remove_link(tweet):
    index = tweet.index('http')
    return tweet[:index]

top_3_tweets['tweet'] = top_3_tweets['tweet'].apply(lambda x: remove_link(x))
top_3_tweets.reset_index(drop=True, inplace=True)

tweet_1 = top_3_tweets['tweet'][0]
tweet_2 = top_3_tweets['tweet'][1]
tweet_3 = top_3_tweets['tweet'][2]

link_1 = top_3_tweets['link'][0]
link_2 = top_3_tweets['link'][1]
link_3 = top_3_tweets['link'][2]

retweets_1 = top_3_tweets['retweets'][0]
retweets_2 = top_3_tweets['retweets'][1]
retweets_3 = top_3_tweets['retweets'][2]

print(tweet_1)
print()
print(tweet_2)
print()
print(tweet_3)