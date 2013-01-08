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
from collections import deque
from twitter import *
from twitter_helper import data_parsers
from twitter_helper import util as twitter_util
from cgi import maxlen


logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger('user')

warnings.filterwarnings('error', category=MySQLdb.Warning)

logger.info( "Reading configurations..")
config = ConfigParser.ConfigParser()
file = config.read('config/twitter_config.cfg')

highpoints = re.compile(u'[\U00010000-\U0010ffff]')
alphanum = re.compile(u'^[\w]+$')

html_parser = HTMLParser.HTMLParser()

DB_HOST             = config.get('DB_Config', 'db_host')
DB_NAME             = config.get('DB_Config', 'db_name')
DB_USER             = config.get('DB_Config', 'db_user')
DB_PASS             = config.get('DB_Config', 'db_password')
CREDS_FILE          = config.get('Twitter_Config', 'twitter_creds')
TWITTER_USERNAME    = config.get('Twitter_Config', 'username')
TWITTER_LISTENER    = config.get('Twitter_Config', 'listener_username')
CONSUMER_KEY        = config.get('Twitter_Config', 'consumer_key')
CONSUMER_SECRET     = config.get('Twitter_Config', 'consumer_secret')
WRITE_RATE          = config.getint('Twitter_Config', 'write_rate')
WARN_RATE           = config.getint('Twitter_Config', 'warn_rate')
#How many hashtags IDs to store in memory
MAX_CACHING_ENTRIES  = config.getint('Twitter_Config', 'max_caching_entries')

TWITTER_CREDS       = os.path.expanduser(CREDS_FILE)




#Try authentication!
oauth_token, oauth_secret = read_token_file(TWITTER_CREDS)
oauth = OAuth( oauth_token, oauth_secret,CONSUMER_KEY,  CONSUMER_SECRET)

#Connection to the Database
logger.info( "Trying to connect to" + DB_HOST +"...")
conn = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASS, db=DB_NAME, charset='utf8' )
cursor = conn.cursor()
logger.info( "...done!")


#Prepare queries
tweet_fields_list = ['id', 'user_id', 'in_reply_to_status_id', 'in_reply_to_user_id', 'favorited', 'retweeted', 'retweet_count', 'lang', 'created_at']
tweet_fields = ', '.join(tweet_fields_list)
tweet_placeholders = ', '.join(['%s']*len(tweet_fields_list))
insert_tweets_sql = 'REPLACE INTO tweet (' + tweet_fields + ') VALUES (' +  tweet_placeholders  + ')'

tweet_text_fields_list = ['tweet_id', 'user_id', 'text', 'geo_lat', 'geo_long', 'place_full_name', 'place_id']
tweet_text_fields = ', '.join(tweet_text_fields_list)
tweet_text_placeholders = ', '.join(['%s']*len(tweet_text_fields_list))
insert_tweets_texts_sql = 'REPLACE INTO tweet_text (' + tweet_text_fields + ') VALUES (' + tweet_text_placeholders + ')'

tweet_url_fields_list = ['tweet_id', 'user_id', 'progressive', 'url']
tweet_url_fields = ', '.join(tweet_url_fields_list)
tweet_url_placeholders = ', '.join(['%s']*len(tweet_url_fields_list))
insert_tweets_urls_sql = 'INSERT INTO tweet_url (' + tweet_url_fields + ') VALUES ( ' + tweet_url_placeholders + ') ON DUPLICATE KEY UPDATE tweet_id=VALUES(tweet_id)'

tweet_hashtag_fields_list = ['tweet_id', 'user_id', 'hashtag_id']
tweet_hashtag_fields = ', '.join(tweet_hashtag_fields_list)
tweet_hashtag_placeholders = ', '.join(['%s']*len(tweet_hashtag_fields_list))
insert_tweets_hashtags_sql = 'REPLACE INTO tweet_hashtag (' + tweet_hashtag_fields + ') VALUES (' + tweet_hashtag_placeholders + ')'

insert_hashtags_sql = 'INSERT INTO hashtag (hashtag, partitioning_value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE hashtag=VALUES(hashtag), partitioning_value=VALUES(partitioning_value)'

user_fields_list = ['id', 'screen_name', 'name', 'verified', 'protected', 'followers_count', 'friends_count', 'statuses_count', 'favourites_count', 'location', 'utc_offset', 'time_zone', 'geo_enabled', 'lang', 'description', 'url', 'created_at']
user_fields = ', '.join(user_fields_list)
user_placeholders = ', '.join(['%s']*len(user_fields_list))
insert_users_sql = 'REPLACE INTO user (' + user_fields + ') VALUES (' + user_placeholders + ') '


#Connecting to Twitter
logger.info( "Connecting to twitter API...")
twitter = Twitter(auth=oauth)
logger.info( "Connecting to the stream...")
twitter_stream = TwitterStream(auth=oauth)
iterator = twitter_stream.statuses.sample()
logger.info("Got connection!")

#This is just for fun
logger.info( "Reading Hamlet, for real!")
text_file  = open("./Hamlet.txt")


########## Use the stream ###############

#Declarations
tweets              = []
tweet_record        = []
tweet_texts         = []
tweet_text_record   = []
urls                = []
hashtags            = []
inserted_hashtags   = {}
hashtag_buffer      = deque(maxlen=MAX_CACHING_ENTRIES)
users               = {}
missing_users       = []

count = 0
total_inserted = 0
time_elapsed = 0
total_time = 0
time_start = 0
last_time_notified = 0


#We update the application status and we notify the Listener account via DM
logger.info( "Tweeting for starters!")
line = twitter_util.prepare_quote(text_file)
now = datetime.datetime.now()
twitter.statuses.update(status=line)
twitter.direct_messages.new(user=TWITTER_LISTENER, text=now.strftime("%Y-%m-%d %H:%M") + " started downloading tweets ")
last_time_notified = time()


#Computation on the Stream
logger.info("Iterating through tweets")
logger.info( "Warn rate is {0} , write rate is {1}".format(WARN_RATE, WRITE_RATE))
try:
    for tweet in iterator:
        time_start = time()
        
        if not data_parsers.contains_fields(tweet, tweet_fields_list) :
            continue
        if not data_parsers.contains_fields(tweet, tweet_text_fields_list) :
            continue

        user_data = []
        if 'user' in tweet:
            user_data = tweet['user']
        else :
            continue

        if not data_parsers.contains_fields(user_data, user_fields_list) :
            continue
            
        if tweet['lang'] == 'en' and  tweet['text'] != None :
    
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
                        valid_hashtag = alphanum.match(hash_text)
                        if valid_hashtag and hash_text not in tweet_hashtags_register :
                            partition = ord(hash_text[0])
                            if not hash_text in inserted_hashtags :
                                cursor.execute(insert_hashtags_sql, [hash_text, partition])
                                conn.commit()
                                hash_id = cursor.lastrowid                                                    
                                
                                if hash_id == None or hash_id == 0 :                                
                                    #Order is inverted as MySQL is not so good in deciding wich check do first 
                                    cursor.execute("SELECT id FROM hashtag h WHERE h.partitioning_value =%s AND h.hashtag = %s", [partition, hash_text])
                                    hash_id = cursor.fetchone()[0]
                                    #Again
                                    if hash_id == None or hash_id == 0 :                            
                                        raise Exception("hash_id is {0} for {1} ".format(hash_id, hash_text) )
                                #else :
                                #    logger.info("Found  {0} in the databse with id {1} ".format(hash_text, hash_id.encode("ascii", "xmlcharrefreplace")))
                                                                
                                
                                
                                if len(hashtag_buffer) >= MAX_CACHING_ENTRIES :
                                    to_remove = hashtag_buffer.popleft()
                                    del inserted_hashtags[to_remove]
                                    
                                hashtag_buffer.append(hash_text)
                                inserted_hashtags[hash_text] = hash_id
                                                             
                            else :
                                hash_id = inserted_hashtags[hash_text]
                            
                            hashtags.append([tweet['id'], user_id, hash_id ])
                            tweet_hashtags_register.append(hash_text)
                            
            time_elapsed = time_elapsed + (time() - time_start)
            
            if count >= WRITE_RATE :
                total_time = time_elapsed 
                time_elapsed = (time_elapsed*1000) /count
                logger.info("Downloading time {0:.5f} secs - 1 tweet rate {1:.2f} millis ".format(total_time, time_elapsed))            
                
                if len(missing_users) > 0 :
                    missing_count = len(missing_users)
                    for user_id in missing_users :
                        if user_id not in users :
                            missing_count = missing_count - 1                    
                    logger.info("Missing {0} users ".format(missing_count))
                                

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

                if total_inserted % WARN_RATE == 0 :
                    logger.info( "Tweeting status!")
                    line = twitter_util.prepare_quote(text_file)
                    now = datetime.datetime.now()
                    twitter.statuses.update(status=line)                   
                    pv_msg = now.strftime("%Y-%m-%d %H:%M") + " Downloaded {0} tweets after {1:.4f} minutes"
                    minutes = (time()-last_time_notified)/60
                    twitter.direct_messages.new(user=TWITTER_LISTENER, text=pv_msg.format(total_inserted, minutes))
                    last_time_notified = time()
                             
                count = 0
                time_elapsed = 0
                                        
                file = config.read('config/twitter_config.cfg')
                WRITE_RATE_TMP = config.getint('Twitter_Config', 'write_rate')
                WARN_RATE_TMP = config.getint('Twitter_Config', 'warn_rate')
                if WRITE_RATE != WRITE_RATE_TMP :
                    logger.info("WRITE RATE changed from {0} to {1} ".format(WRITE_RATE, WRITE_RATE_TMP))
                    WRITE_RATE = WRITE_RATE_TMP
                if WARN_RATE != WARN_RATE_TMP :
                    logger.info("WARN RATE changed from {0} to {1} ".format(WARN_RATE, WARN_RATE_TMP))
                    WARN_RATE = WARN_RATE_TMP
                    
except Exception as e:
    conn.rollback()
    if hasattr(cursor, '_last_executed') :                
        logger.error("An error occurred while exectuing the query:")    
        logger.error(cursor._last_executed)
    else :
        logger.error("An error occurred while parsing tweets:")
        
    logger.error(e)
    cursor.close()
    
    logger.info("Warn your master!")
    now = datetime.datetime.now()
    error_type = "{0}]".format(type(e))
    error_message = "[ERROR: " + error_type + " "
    pv_msg = now.strftime("%Y-%m-%d %H:%M") + error_message + "Application is shuttin down after {0} tweets!"
    twitter.direct_messages.new(user=TWITTER_LISTENER,text=pv_msg.format(total_inserted))

                                                                        
    #else :
    #    print "What's this!?"
    #    print tweet
    #    break

print "-------"
print count
cursor.close()




#Todo Save to DB
#cursor.lastrowid
