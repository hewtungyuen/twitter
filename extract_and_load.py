import tweepy
import pandas as pd
import datetime

# authentication
from keys import ACCESS_TOKEN, ACCESS_TOKEN_SECRET, API_KEY, API_KEY_SECRET

auth = tweepy.OAuthHandler(API_KEY, API_KEY_SECRET)
auth.set_access_token(ACCESS_TOKEN,ACCESS_TOKEN_SECRET)
api = tweepy.API(auth)

# function to convert datetime object to format accepted by tweepy 
def convert_format(datetime_obj):
    year = str(datetime_obj.year)
    month = '0' + str(datetime_obj.month)
    day = str(datetime_obj.day)
    hour = str(datetime_obj.hour)
    minute = str(datetime_obj.minute)

    return year + month + day + '0000'


today = datetime.datetime.today()
yesterday = today - datetime.timedelta(1)

today = yesterday
today = convert_format(today)

# yesterday = convert_format(yesterday)
# print(yesterday)

# obtaining tweets on today's date 
tweets = api.search_30_day(label = 'twitterETL', query='from:WSJ', fromDate = today, maxResults = 10)

# database connection
import mysql.connector
from password import USERNAME, PASSWORD

mydb = mysql.connector.connect(
  host="localhost",
  user=USERNAME,
  password=PASSWORD,
  database = 'twitter'
)

mycursor = mydb.cursor()

# storing data in respective lists
for tweet in tweets:
    try:
        created_at = tweet.created_at
        retweet_count = tweet.retweet_count
        full_text = tweet.extended_tweet['full_text']

        mycursor.execute(
            """
            INSERT INTO tweets (`tweet`,`retweets`,`timestamp`) VALUES (%s,%s,%s);
            """,
            (full_text,retweet_count,created_at)
        )
        mydb.commit()
    except:
        pass