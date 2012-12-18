import gc
import os
import sys
import logging
import datetime
import dateutil.parser as parser
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

tweet_fields_list = ['id', 'user_id', 'in_reply_to_status_id', 'in_reply_to_user_id', 'favorited', 'retweeted', 'retweet_count', 'lang', 'created_at']
tweet_fields = ', '.join(tweet_fields_list)
tweet_placeholders = ', '.join(['%s']*len(tweet_fields_list))
insert_tweets_sql = 'INSERT INTO tweet (' + tweet_fields + ') VALUES (' +  tweet_placeholders  + ')'

tweet_text_fields_list = ['tweet_id', 'user_id', 'text', 'lat', 'long', 'place_full_name', 'place_id']
tweet_text_fields = ', '.join(tweet_text_fields_list)
tweet_text_placeholders = ', '.join(['%s']*len(tweet_text_fields_list))
insert_tweets_texts_sql = 'INSERT INTO tweet_text (' + tweet_text_fields + ') VALUES (' + tweet_text_placeholders + ')'


tweet_url_fields_list = ['tweet_id', 'progressive', 'url']
tweet_url_fields = ', '.join(tweet_url_fields_list)
tweet_url_placeholders = ', '.join(['%s']*len(tweet_url_fields_list))
insert_tweets_urls_sql = 'INSERT INTO tweet_url (' + tweet_url_fields + ') VALUES ( ' + tweet_url_placeholders + ')'

tweet_hashtag_fields_list = ['tweet_id', 'user_id', 'hashtag_id']
tweet_hashtag_fields = ', '.join(tweet_hashtag_fields_list)
tweet_hashtag_placeholders = ', '.join(['%s']*len(tweet_hashtag_fields_list))
insert_tweets_hashtags_sql = 'INSERT INTO tweet_hashtag (' + tweet_hashtag_fields + ') VALUES (' + tweet_hashtag_placeholders + ')'


insert_hashtags_sql = 'INSERT INTO tweet_hashtag (hashtag) VALUES (%s)'

user_fields_list = ['id', 'screen_name', 'name', 'verified', 'protected', 'followers_count', 'friends_count', 'statuses_count', 'favourites_count', 'location', 'utc_offset', 'time_zone', 'geo_enabled', 'lang', 'description', 'url', 'created_at']
user_fields = ', '.join(user_fields_list)
user_placeholders = ', '.join(['%s']*len(user_fields_list))

insert_users_sql = 'INSERT INTO tweet (' + user_fields + ') VALUES (' + user_placeholders + ')'





logger.info( "Connecting to the stream...")
twitter_stream = TwitterStream(auth=oauth)
iterator = twitter_stream.statuses.sample()

# Use the stream
timer = datetime.datetime.now()

tweets = []
tweet_record = []
tweet_texts = []
urls = {}
hashtags = {}
users = {}


count = 0
for tweet in iterator:
    if 'text' in tweet  and  tweet['text'] != None and tweet['lang'] == 'en' :
        count = count + 1
        
        for field in tweet_fields_list :
            if field == 'user_id' :
                tweet_record.append(tweet['user']['id'])
            if field == 'created_at' :
                datetime = parser.parse(tweet['created_at'])
                datetime = datetime.isoformat(' ')[:-6]
                tweet_record.append(datetime)
            elif field in tweet :                                
                tweet_record.append(tweet[field])
            else:
                print field
                print '++'
                print tweet
                print '++'
                print tweet.keys()
                break

        if len(tweet['entities']) >0 and len(tweet['entities']['urls']) > 0  :
            for url in tweet['entities']['urls'] :
                print url
            for hash in tweet['entities']['hashtags'] :
                print hash

        if count > 5 :
            break
    #else :
    #    print "What's this!?"
    #    print tweet
    #    break
             
print "-------"
print count

#Todo Save to DB
