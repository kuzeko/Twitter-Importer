import gc
import os
import re
import sys
import warnings
import logging
import datetime
import dateutil.parser as parser
import ConfigParser
import MySQLdb
import HTMLParser

from time import time
from twitter import *
from twitter_helper import data_parsers

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger('user')

warnings.filterwarnings('error', category=MySQLdb.Warning)

logger.info( "Reading configurations..")
config = ConfigParser.ConfigParser()
file = config.read('config/twitter_config.cfg')

highpoints = re.compile(u'[\U00010000-\U0010ffff]')
html_parser = HTMLParser.HTMLParser()

DB_HOST             = config.get('DB_Config', 'db_host')
DB_NAME             = config.get('DB_Config', 'db_name')
DB_USER             = config.get('DB_Config', 'db_user')
DB_PASS             = config.get('DB_Config', 'db_password')
CREDS_FILE          = config.get('Twitter_Config', 'twitter_creds')
TWITTER_USERNAME    = config.get('Twitter_Config', 'username')
CONSUMER_KEY        = config.get('Twitter_Config', 'consumer_key')
CONSUMER_SECRET     = config.get('Twitter_Config', 'consumer_secret')
TWITTER_CREDS       = os.path.expanduser(CREDS_FILE)

#How many hashtags IDs to store in memory
MAX_CACHING_ENTRIES = 10000

oauth_token, oauth_secret = read_token_file(TWITTER_CREDS)
oauth = OAuth( oauth_token, oauth_secret,CONSUMER_KEY,  CONSUMER_SECRET)


logger.info( "Trying to connect to" + DB_HOST +"...")
conn = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASS, db=DB_NAME, charset='utf8' )
cursor = conn.cursor()
logger.info( "...done!")

tweet_fields_list = ['id', 'user_id', 'in_reply_to_status_id', 'in_reply_to_user_id', 'favorited', 'retweeted', 'retweet_count', 'lang', 'created_at']
tweet_fields = ', '.join(tweet_fields_list)
tweet_placeholders = ', '.join(['%s']*len(tweet_fields_list))
insert_tweets_sql = 'INSERT INTO tweet (' + tweet_fields + ') VALUES (' +  tweet_placeholders  + ')'

tweet_text_fields_list = ['tweet_id', 'user_id', 'text', 'geo_lat', 'geo_long', 'place_full_name', 'place_id']
tweet_text_fields = ', '.join(tweet_text_fields_list)
tweet_text_placeholders = ', '.join(['%s']*len(tweet_text_fields_list))
insert_tweets_texts_sql = 'INSERT INTO tweet_text (' + tweet_text_fields + ') VALUES (' + tweet_text_placeholders + ')'

tweet_url_fields_list = ['tweet_id', 'user_id', 'progressive', 'url']
tweet_url_fields = ', '.join(tweet_url_fields_list)
tweet_url_placeholders = ', '.join(['%s']*len(tweet_url_fields_list))
insert_tweets_urls_sql = 'INSERT INTO tweet_url (' + tweet_url_fields + ') VALUES ( ' + tweet_url_placeholders + ') ON DUPLICATE KEY UPDATE tweet_id=VALUES(tweet_id)'

tweet_hashtag_fields_list = ['tweet_id', 'user_id', 'hashtag_id']
tweet_hashtag_fields = ', '.join(tweet_hashtag_fields_list)
tweet_hashtag_placeholders = ', '.join(['%s']*len(tweet_hashtag_fields_list))
insert_tweets_hashtags_sql = 'INSERT INTO tweet_hashtag (' + tweet_hashtag_fields + ') VALUES (' + tweet_hashtag_placeholders + ')'

insert_hashtags_sql = 'INSERT INTO hashtag (hashtag) VALUES (%s) ON DUPLICATE KEY UPDATE hashtag=VALUES(hashtag)'

user_fields_list = ['id', 'screen_name', 'name', 'verified', 'protected', 'followers_count', 'friends_count', 'statuses_count', 'favourites_count', 'location', 'utc_offset', 'time_zone', 'geo_enabled', 'lang', 'description', 'url', 'created_at']
user_fields = ', '.join(user_fields_list)
user_placeholders = ', '.join(['%s']*len(user_fields_list))
insert_users_sql = 'REPLACE INTO user (' + user_fields + ') VALUES (' + user_placeholders + ') '





logger.info( "Connecting to the stream...")
twitter_stream = TwitterStream(auth=oauth)
iterator = twitter_stream.statuses.sample()
logger.info("Got connection!")

# Use the stream
tweets              = []
tweet_record        = []
tweet_texts         = []
tweet_text_record   = []
urls                = []
hashtags            = []
inserted_hashtags   = {}
users               = {}
missing_users       = []



count = 0
total_inserted = 0
time_elapsed = 0
time_start = 0
logger.info("Iterating through tweets")
for tweet in iterator:
    time_start = time()
    if ('text' in tweet and 'lang' in tweet) and (tweet['lang'] == 'en' and  tweet['text'] != None ) :

        tweet_record = []
        tweet_text_record   = []

        user_data = tweet['user']
        user_id = user_data['id']

        tweet_record = data_parsers.parse_tweet_basic_infos(tweet, tweet_fields_list)        
        tweets.append(tweet_record)

        tweet_text_record = data_parsers.parse_tweet_text_infos(tweet, tweet_text_fields_list )
        tweet_texts.append(tweet_text_record)


        user_record = []
        user_record = data_parsers.parse_user_infos(user_data, user_fields_list)
        if user_record == None :
            missing_users.append(user_id)
        else :
            users[user_id]=user_record

        #To avoid duplicates
        tweet_hashtags_register = []
        count = count + 1
        
        if len(tweet['entities']) >0 :
            if len(tweet['entities']['urls']) > 0  :
                url_count = 0
                for url in tweet['entities']['urls'] :
                    url_count = url_count + 1
                    urls.append([tweet['id'], user_id, url_count, url['expanded_url']])


            if len(tweet['entities']['hashtags']) > 0  :
                for hash in tweet['entities']['hashtags'] :
                    hash_id = 0                    
                    hash_text = highpoints.sub(u'', hash['text'])
                    hash_text = hash_text.lower()
                    if hash_text not in tweet_hashtags_register :
                        if not hash_text in inserted_hashtags :
                            cursor.execute(insert_hashtags_sql, [hash_text])
                            conn.commit()
                            hash_id = cursor.lastrowid                                                    
                            
                            if hash_id == None or hash_id == 0 :                                
                                cursor.execute("SELECT id FROM hashtag h WHERE h.hashtag = %s", [hash_text])
                                hash_id = cursor.fetchone()[0]
                                #Again
                                if hash_id == None or hash_id == 0 :                            
                                    raise Exception("hash_id is {0} for {1} ".format(hash_id, hash_text) )
                            #else :
                            #    logger.info("Found  {0} in the databse with id {1} ".format(hash_text, hash_id.encode("ascii", "xmlcharrefreplace")))
                                                            
                            inserted_hashtags[hash_text] = hash_id                             
                        else :
                            hash_id = inserted_hashtags[hash_text]
                        
                        hashtags.append([tweet['id'], user_id, hash_id ])
                        tweet_hashtags_register.append(hash_text)
                        
        time_elapsed = time_elapsed + (time() - time_start)
        
        if count > 1000 :
                
            time_elapsed = time_elapsed /count
            logger.info("Downaloading time rate {0} ".format(time_elapsed))            
            
            if len(missing_users) > 0 :
                missing_count = len(missing_users)
                for user_id in missing_users :
                    if user_id not in users :
                        missing_count = missing_count - 1                    
                logger.info("Missing {0} users ".format(missing_count))
                
            
            try:
                #logger.info("Inserting {0} tweets and {1} texts ".format(len(tweets), len(tweet_texts)))
                time_start = time()
                cursor.executemany(insert_tweets_sql, tweets)            
                cursor.executemany(insert_tweets_texts_sql, tweet_texts)                
                
                #logger.info("Inserting {0} tweet urls ".format(len(urls)))
                cursor.executemany(insert_tweets_urls_sql, urls)
                
                #logger.info("Inserting {0} tweet hashtags ".format(len(hashtags)))
                cursor.executemany(insert_tweets_hashtags_sql, hashtags)
                
                list_users = users.values()
                #logger.info("Inserting {0} users ".format(len(list_users)))                
                cursor.executemany(insert_users_sql, list_users)
                
                logger.info("Commit..")
                conn.commit()
                
                time_elapsed = (time() - time_start)
                logger.info("Queries executed in {0} seconds ".format(time_elapsed))
                
                tweets              = []
                tweet_texts         = []
                urls                = []
                hashtags            = []
                users               = {}

                if len(inserted_hashtags) > MAX_CACHING_ENTRIES :
                    inserted_hashtags = {}                                                    

                total_inserted = total_inserted + count
                logger.info("Inserted {0} tweets up to now ".format(total_inserted))                

                 
                count = 0
                time_elapsed = 0
                
            except Exception as e:
                conn.rollback()                
                logger.error("An error occurred while exectuing the query:")
                logger.error(cursor._last_executed)
                logger.error(e)
                cursor.close()

                break
    #else :
    #    print "What's this!?"
    #    print tweet
    #    break

print "-------"
print count
cursor.close()




#Todo Save to DB
#cursor.lastrowid
