import gc
import os
import sys
import logging
import ConfigParser
import MySQLdb

from twitter import *

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger('user')

logger.info( "Reading configurations..")
config = ConfigParser.ConfigParser()
file = config.read('config/twitter_config.cfg')

DB_HOST             = config.get('DB_Config', 'db_host')
DB_NAME             = config.get('DB_Config', 'db_name')
DB_USER             = config.get('DB_Config', 'db_user')
DB_PASS             = config.get('DB_Config', 'db_password')
CREDS_FILE          = config.get('Twitter_Config', 'twitter_creds')
TWITTER_USERNAME    = config.get('Twitter_Config', 'username')
CONSUMER_KEY        = config.get('Twitter_Config', 'consumer_key')
CONSUMER_SECRET     = config.get('Twitter_Config', 'consumer_secret')
TWITTER_CREDS       = os.path.expanduser(CREDS_FILE)

oauth_token, oauth_secret = read_token_file(TWITTER_CREDS)
oauth = OAuth( oauth_token, oauth_secret,CONSUMER_KEY,  CONSUMER_SECRET)


logger.info( "Trying to connect to" + DB_HOST +"...")
conn = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASS, db=DB_NAME)
cursor = conn.cursor()
logger.info( "...done!")

insert_tweets_sql = """INSERT INTO tweet (id, user_id, in_reply_to_status_id, in_reply_to_user_id, favorited, retweeted, retwet_count, lang, created_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""

insert_tweets_texts_sql = """INSERT INTO tweet_text (tweet_id, user_id, text, lat, long, place_full_name, place_id)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)"""

insert_tweets_urls_sql = """INSERT INTO tweet_url (tweet_id, user_id, text, lat, long, place_full_name, place_id)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)"""



logger.info( "Connecting to the stream...")
twitter_stream = TwitterStream(auth=oauth)
iterator = twitter_stream.statuses.sample()

# Use the stream

count = 0
for tweet in iterator:
    if 'text' in tweet  and  tweet['text'] != None and tweet['lang'] == 'en' :
        print tweet['text']
        if len(tweet['entities']) >0 and len(tweet['entities']['urls']) > 0  :
            for url in tweet['entities']['urls'] :
                print url
    	count = count + 1
print "-------"
print count

#Todo Save to DB
