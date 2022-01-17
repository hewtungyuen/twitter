# pip install "apache-airflow==2.2.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.2.3/constraints-3.8.txt"

# mongodb connection
from pymongo import MongoClient

def databaseConnection():
    uri = "mongodb+srv://cluster0.71gzj.mongodb.net/twitter?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority"
    client = MongoClient(uri,
                        tls=True,
                        tlsCertificateKeyFile= r'X509-cert-6071320618069174467.pem')

    db = client['twitter']
    collection = db['tweets']

    print('Database connected.')
    return collection

# authentication
from keys import ACCESS_TOKEN, ACCESS_TOKEN_SECRET, API_KEY, API_KEY_SECRET
import tweepy
import datetime 

# function to convert datetime object to format accepted by tweepy 
def convert_format(datetime_obj):
    year = str(datetime_obj.year)
    month = '0' + str(datetime_obj.month)
    day = str(datetime_obj.day)

    return year + month + day + '0000'

# function to remove link from a tweet 
def remove_link(tweet):
    index = tweet.index('http')
    return tweet[:index]

# extracting data using tweepy
def extract():
    auth = tweepy.OAuthHandler(API_KEY, API_KEY_SECRET)
    auth.set_access_token(ACCESS_TOKEN,ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)

    # obtaining yesterday's date
    yesterday = datetime.datetime.today() - datetime.timedelta(1)
    yesterday = convert_format(yesterday)

    # obtaining tweets from yesterday's date until now
    tweets = api.search_30_day(label = 'twitterETL', query='from:WSJ', fromDate = yesterday, maxResults = 10)

    print('Tweets extracted.')
    return tweets

# storing data in MongoDB
def load(collection, tweets):
    for tweet in tweets:
        try:
            timestamp = tweet.created_at
            retweet_count = tweet.retweet_count
            full_tweet = tweet.extended_tweet['full_text']

            collection.insert_one({'timestamp': timestamp, 'retweet_count': retweet_count, 'full_tweet': full_tweet})
        except:
            pass

    print('Tweets loaded into MongoDB.')

# obtaining data from MongoDB, then returning the top 3 tweets according to retweet count
def transform(collection):
    yesterday = datetime.datetime.today() - datetime.timedelta(1)

    # taking data out of mongodb 
    data = collection.find({'timestamp': {'$gte': yesterday}}).sort('retweet_count',-1).limit(3)

    top_tweets = []

    for d in data:
        retweet_count = d['retweet_count']
        full_tweet = d['full_tweet']
        link = full_tweet.split(' ')[-1]

        s = f'Tweet: {remove_link(full_tweet)}. \n Retweet count: {retweet_count}. \n Article link: {link} \n'

        top_tweets.append(s)

    tweet_1 = top_tweets[0]
    tweet_2 = top_tweets[1]
    tweet_3 = top_tweets[2]

    email_text = f"""
    The top 3 tweets for the day are: \n
    {tweet_1} \n
    {tweet_2} \n
    {tweet_3} 
    """.encode()

    return email_text

import smtplib
from keys import EMAIL_PASSWORD

def send_email(email_text):
    server = smtplib.SMTP('smtp.gmail.com',587)
    server.starttls()
    server.login('twitter.email.project@gmail.com',EMAIL_PASSWORD)
    server.sendmail('twitter.email.project@gmail.com','twitter.email.project@gmail.com',email_text)
    print('Email sent.')

# overall
def twitter_elt():
    collection = databaseConnection()
    tweets = extract()
    load(collection, tweets)
    email_text = transform(collection)
    send_email(email_text)

if __name__ == '__main__':
    twitter_elt()