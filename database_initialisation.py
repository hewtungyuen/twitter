# from extract import timestamp, retweets, text
import mysql.connector

from password import USERNAME, PASSWORD

mydb = mysql.connector.connect(
  host="localhost",
  user=USERNAME,
  password=PASSWORD,
  database = 'twitter'
)

mycursor = mydb.cursor()

# mycursor.execute("CREATE DATABASE twitter")

mycursor.execute(
    """
    CREATE TABLE tweets (
        id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
        tweet VARCHAR(255),
        retweets INT(255),
        timestamp DATETIME
    );
    """
)