# Twitter ELT pipeline

The aim of this project is to develop an ELT (extract, load, transform) pipeline on Twitter tweets from <a href = https://twitter.com/WSJ>@WSJ</a> to preodically process and email the top tweets of the day according to retweet count. I used the tweepy library to extract the tweets, MongoDB to store them, the smtplib library to send e-mails, and Apache Airflow for job scheduling. 
