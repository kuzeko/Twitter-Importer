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


from twitter import *

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger('user')

warnings.filterwarnings('error', category=MySQLdb.Warning)

logger.info( "Reading configurations..")
config = ConfigParser.ConfigParser()
file = config.read('config/twitter_config.cfg')

highpoints = re.compile(u'[\U00010000-\U0010ffff]')


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
insert_users_sql = 'INSERT INTO user (' + user_fields + ') VALUES (' + user_placeholders + ')'





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


count = 0
total_inserted = 0

logger.info("Iterating through tweets")
for tweet in iterator:
    if 'text' in tweet  and  tweet['text'] != None and tweet['lang'] == 'en' :

        tweet_record = []
        tweet_text_record   = []

        user_data = tweet['user']
        user_id = user_data['id']

        for field in tweet_fields_list :
            if field == 'user_id' :
                tweet_record.append(user_id)
            elif field == 'created_at' :
                datetime = parser.parse(tweet['created_at'])
                datetime = datetime.isoformat(' ')[:-6]
                tweet_record.append(datetime)
            elif field in tweet :
                if tweet[field] == None :
                    value = 0
                else :
                    value = tweet[field]
                tweet_record.append(value)
        tweets.append(tweet_record)

        for field in tweet_text_fields_list :
            if field == 'tweet_id' :
                tweet_text_record.append(tweet['id'])
            elif field == 'user_id' :
                tweet_text_record.append(user_id)
            elif field == 'text' :
                value = tweet['text'].strip()
                value = highpoints.sub(u'', value)
                tweet_text_record.append(value)
            elif field == 'geo_lat' :
                if tweet['geo'] != None:
                    tweet_text_record.append(tweet['geo']['coordinates'][0])
                else :
                    tweet_text_record.append(0)
            elif field == 'geo_long' :
                if tweet['geo'] != None :
                    tweet_text_record.append(tweet['geo']['coordinates'][1])
                else :
                    tweet_text_record.append(0)
            elif field == 'place_full_name' :
                if tweet['place'] != None :
                    tweet_text_record.append(tweet['place']['full_name'])
                else :
                    tweet_text_record.append('')
            elif field == 'place_id' :
                # http://api.twitter.com/1/geo/id/6b9ed4869788d40e.json
                if tweet['place'] != None :
                    tweet_text_record.append(tweet['place']['id'])
                else :
                    tweet_text_record.append('')
            elif field in tweet :
                if tweet[field] == None :
                    value = 0
                else :
                    value = tweet[field]
                tweet_text_record.append(value)
        tweet_texts.append(tweet_text_record)


        user_record = []

        for field in user_fields_list :
            if field == 'created_at' :
                datetime = parser.parse(user_data['created_at'])
                datetime = datetime.isoformat(' ')[:-6]
                user_record.append(datetime)
            elif field == 'description' :
                value = user_data['description'].strip()
                value = highpoints.sub(u'', value)
                user_record.append(value)
            elif field == 'utc_offset' :
                if user_data['utc_offset'] == None or  user_data['utc_offset'] == '':
                    user_record.append(0)
                else :
                    user_record.append(user_data['utc_offset'])
            elif field in user_data :
                if user_data[field] == None :
                    value = ''
                else :
                    value = user_data[field]
                user_record.append(value)
        users[user_id]=user_record


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
                    if not hash['text'] in inserted_hashtags :
                        cursor.execute(insert_hashtags_sql, [hash['text']])
                        conn.commit()
                        cursor_lastrowid = cursor.lastrowid                        
                        inserted_hashtags[hash['text']] = hash_id =  cursor_lastrowid
                        
                        if cursor_lastrowid == None or cursor_lastrowid == 0 :
                            logger.info("Looking for  {0} in the databse ".format(hash['text']))
                            cursor.execute("SELECT id FROM hashtag h WHERE h.hashtag = %s", [hash['text']])
                            inserted_hashtags[hash['text']] = hash_id = cursor.fetchone()[0]
                            
                        if hash_id == None or hash_id == 0 :                            
                            raise Exception("hash_id is {0} for {1} ".format(hash_id, hash['text']) )
                    else :
                        hash_id = inserted_hashtags[hash['text']]
                    
                        
                    hashtags.append([tweet['id'], user_id, hash_id ])


        if count > 1000 :
            try:
                logger.info("Inserting {0} tweets ".format(len(tweets)))
                cursor.executemany(insert_tweets_sql, tweets)
                
                logger.info("Inserting {0} tweet texts ".format(len(tweet_texts)))
                cursor.executemany(insert_tweets_texts_sql, tweet_texts)
                
                logger.info("Inserting {0} tweet urls ".format(len(urls)))
                cursor.executemany(insert_tweets_urls_sql, urls)
                
                logger.info("Inserting {0} tweet hashtags ".format(len(hashtags)))
                cursor.executemany(insert_tweets_hashtags_sql, hashtags)
                
                list_users = users.values()
                logger.info("Inserting {0} users ".format(len(list_users)))                
                cursor.executemany(insert_users_sql, list_users)
                
                logger.info("Commit..")
                conn.commit()
                
                tweets              = []
                tweet_texts         = []
                urls                = []
                hashtags            = []
                inserted_hashtags   = {}
                users               = {}

                total_inserted = total_inserted + count
                count = 0
                logger.info("Inserted {0} tweets up to now ".format(total_inserted))                

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
