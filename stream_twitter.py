import os
import re
import logging
import datetime
import traceback
import Queue
import ConfigParser
import HTMLParser
import threading
import urllib2
import time
import gc

from twitter import *

from twitter_helper import util as twitter_util
from twitter_helper.twitter_data import TwitterData
from twitter_helper.mysql_connector import MysqlTwitterConnector as DBConnector
#from twitter_helper.status_monitor import ProcessMonitor


""" Setup the logger """
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger('user')


""" Parse Config File """
logger.info("Reading configurations..")
config = ConfigParser.ConfigParser()
file = config.read('config/twitter_config.cfg')

DB_HOST             = config.get('DB_Config', 'db_host')
DB_NAME             = config.get('DB_Config', 'db_name')
DB_USER             = config.get('DB_Config', 'db_user')
DB_PASS             = config.get('DB_Config', 'db_password')
CREDS_FILE          = config.get('Twitter_Config', 'twitter_creds')
CONSUMER_KEY        = config.get('Twitter_Config', 'consumer_key')
CONSUMER_SECRET     = config.get('Twitter_Config', 'consumer_secret')
WRITE_RATE          = config.getint('Twitter_Config', 'write_rate')
WARN_RATE           = config.getint('Twitter_Config', 'warn_rate')
DM_NOTIFICATIONS    = config.getboolean('Twitter_Config', 'direct_message_notification')
TWITTER_LISTENER    = config.get('Twitter_Config', 'listener_username')
FILTER_LANG         = config.get('Twitter_Config', 'language').split(',')
DEMO                = config.getboolean('Twitter_Config', 'demo_mode')

""" How many hashtags IDs to store in memory """
MAX_CACHING_ENTRIES = config.getint('Twitter_Config', 'max_caching_entries')
TWITTER_CREDS       = os.path.expanduser(CREDS_FILE)

if DEMO :
    logger.info("Running in Demo Mode: No connection the Database - Tweets will not be saved!")
else:
    """ Test Connection to the Database """
    db_active = False
    logger.info("Trying to connect to " + DB_NAME + " on " + DB_HOST + "...")
    db_active = DBConnector.test(host=DB_HOST, user=DB_USER, passwd=DB_PASS, db=DB_NAME, charset='utf8')
    if not db_active:
        logger.error("An error occurred while connecting to the database")
        exit(2)
    logger.info("...done!")

""" Twitter auth Credentials """
oauth_token, oauth_secret = read_token_file(TWITTER_CREDS)
oauth_auth_mode = OAuth(oauth_token, oauth_secret, CONSUMER_KEY, CONSUMER_SECRET)

""" Connection with OAuth: this is used for DM and Tweeting """
if DM_NOTIFICATIONS:
    logger.info("Connecting to Twitter REST API...")
    twitter = Twitter(auth=oauth_auth_mode)
    logger.info("Authentication mode for REST API: OAuth")

""" Connection with OAuth: this is currently the only way """
logger.info("Connecting to the stream...")
twitter_stream = TwitterStream(auth=oauth_auth_mode, block=False)
logger.info("Authentication mode for Stream: OAuth")

""" Logging Variables """
iteration_count     = 0
inserted_count      = 0
skipped_count       = 0
total_inserted      = 0
last_time_notified  = 0
connection_counter  = 0

""" Notify via DM the starting of the activities """
if DM_NOTIFICATIONS:
    """ We update the application status and we notify the Listener account via DM """
    logger.info("Tweeting notification for starters!")

    """ This is just for fun """
    logger.info("Reading Hamlet, for real!")
    text_file = open("./Hamlet.txt")
    line = twitter_util.prepare_quote(text_file)
    now = datetime.datetime.now()
    logger.info("Posting on twitter")
    twitter.statuses.update(status=line)
    """ No more fun """

    dm_text = now.strftime("%Y-%m-%d %H:%M") + " started downloading tweets "
    twitter.direct_messages.new(user=TWITTER_LISTENER, text=dm_text)
    last_time_notified = time.time()

logger.info("Warn rate is {0} , write rate is {1}".format(WARN_RATE, WRITE_RATE))
try:
    continue_download = True
    skip_tweet = False
    application_start_time = time.time()

    # """ Setup a messaging queue to track activities """
    # message_queue = Queue.PriorityQueue()
    # monitoring_job = threading.Thread(target=ProcessMonitor.print_messages,
    #                                   args=(message_queue, logger))
    # monitoring_job.daemon = True
    # monitoring_job.start()

    """ Use the stream """
    while continue_download:
        try:
            """ Measure time """
            time_start = time.time()

            """ Size of the buffer of tweets to write in the DB """
            buffer_size = WRITE_RATE
            """ Prepare parser with some larger buffer size """
            data_parser = TwitterData(buffer_size + (buffer_size/100))

            if connection_counter == 0":
            """ Reset """
                iterator = None
                gc.collect()


                """ Get the tweets - this is also important to be refreshed every now and then """
                iterator = twitter_stream.statuses.sample()
                logger.info("Got Stream connection!")

            connection_counter = (connection_counter + 1) % 5


            """ Computation on the Stream """
            logger.info("Iterating through tweets")
            none_tweets = 0
            for tweet in iterator:
                if tweet is None:
                    none_tweets += 1
                    if none_tweets > 50000:
                        logger.info("More than 50000 Null tweets, restarting")
                        break
                    if none_tweets % 1000 == 0:
                        logger.info("More than 1000 Null tweets, sleeping")
                        time.sleep(5.2)
                    if none_tweets % 100 == 0:
                        time.sleep(3.2)
                    continue
                none_tweets = 0
                iteration_count +=  + 1
                """ Did we skip last tweet? """
                if skip_tweet:
                    skipped_count += 1
                    skip_tweet = False

                if iteration_count % WRITE_RATE == 0:
                    logger.info("Skipped {0} objects, Inserted {1} and Downloaded {2}".format(skipped_count, inserted_count, iteration_count))

                """ Check if the tweet contains all the necessary fields """
                skip_tweet = not data_parser.contains_fields(tweet, TwitterData.tweet_fields_list,  ['user_id'])
                skip_tweet = skip_tweet or not data_parser.contains_fields(tweet, ['text'])

                """ if it doesn't: skip it """
                if skip_tweet:
                    continue

                user_data = []
                skip_tweet = not 'user' in tweet
                if not skip_tweet:
                    user_data = tweet['user']
                else:
                    continue

                skip_tweet = not data_parser.contains_fields(user_data, TwitterData.user_fields_list)
                skip_tweet = skip_tweet or tweet['text'] is None
                """ if all fields are in place and also the language of the text is acceptable """
                if not skip_tweet and ('lang' in tweet and ("None" in FILTER_LANG or tweet['lang'] in FILTER_LANG)):

                    if DEMO:
                        logger.info("Retrieved: " + tweet['text'])
                        success = True
                    else:
                        """ put the tweet in the queue to be inserted later """
                        success = data_parser.enqueue_tweet_data(tweet)

                    if success:
                        inserted_count += 1
                        """ If buffer is full stop downloading and start a writing thread """
                        if inserted_count >= WRITE_RATE:
                            break

            """ Print some stats """
            time_elapsed = (time.time() - time_start)
            """ Convert to millis and divide for each tweet """
            time_per_tweet = (time_elapsed*1000) / (1 + inserted_count)
            time_iteration = (time_elapsed*1000) / (1 + iteration_count)
            logger.info("Downloading time {0:.5f} secs - 1 tweet rate {1:.3f} millis - 1 iteration rate {2:.3f} millis ".format(time_elapsed, time_per_tweet, time_iteration))

            """ Notify when it may be stuck """
            if DM_NOTIFICATIONS and skipped_count > 50000 and (time.time() - last_time_notified)/(60*60) > 18:
                try:
                    logger.info("Tweeting status!")
                    prv_msg = now.strftime("%Y-%m-%d %H:%M") + " System may be stuck: Downloaded {0} and skipped {1} tweets after {2:.4f} minutes"
                    minutes = (time.time()-last_time_notified)/60
                    logger.info(prv_msg.format(total_inserted, skipped_count, minutes))
                    twitter.direct_messages.new(user=TWITTER_LISTENER, text=prv_msg.format(total_inserted, skipped_count, minutes))
                    last_time_notified = time.time()
                except Exception as e:
                    logger.error("An error occurred while notifying:")
                    trace = traceback.format_exc()
                    logger.error(trace)
                    """ it is not critical, better wait and try again """
                    time.sleep(15)


            """ reset the error count """
            skipped_count = 0
            inserted_count = 0
            iteration_count = 0
        except (urllib2.HTTPError, urllib2.URLError) as e:
            logger.error("An error occurred while downloading tweets:")
            trace = traceback.format_exc()
            logger.error(trace)
            """ Wait a bit before trying something else """
            time.sleep(35.2)
            if DM_NOTIFICATIONS:
                """ Send a DM to notify of the problem """
                logger.info("Warn your master!")
                now = datetime.datetime.now()
                error_type = "{0}]".format(e.__class__.__name__)
                error_message = "[ERROR: " + error_type + " "
                dm_text = now.strftime("%Y-%m-%d %H:%M") + error_message + "Application is trying to continue after {0} tweets!"
                dm_text = dm_text.format(total_inserted)
                logger.info("Sending: " + dm_text)
                twitter.direct_messages.new(user=TWITTER_LISTENER, text=dm_text)

        if not DEMO:
            logger.info("Creating connector to the Database")
            logger.info("Opening connection to DB " + DB_NAME + " on " + DB_HOST + "...")
            connector = DBConnector(host=DB_HOST, user=DB_USER, passwd=DB_PASS, db=DB_NAME, charset='utf8', data_parser=data_parser, cache_size=MAX_CACHING_ENTRIES)
            logger.info("...done!")
            """ Get the queues """
            tweets = data_parser.tweets_queue
            tweet_texts = data_parser.tweet_texts_queue
            users = data_parser.users_queue
            urls = data_parser.urls_queue
            hashtags = data_parser.hashtags_queue

            total_inserted += tweets.qsize()
            logger.info("Starting insertion job")
            db_job = threading.Thread(target=connector.insert_records,
                                      args=(tweets, tweet_texts, users, urls, hashtags, logger))
            db_job.start()

            logger.info("Downloaded for insertion {0} tweets up to now after {1} secs ".format(total_inserted, (time.time() - application_start_time)))

            if DM_NOTIFICATIONS and (time.time() - last_time_notified)/(60*60) > 18:
                try:
                    logger.info("Tweeting status!")

                    """ This is just for fun """
                    logger.info("Reading Hamlet, for real!")
                    text_file = open("./Hamlet.txt")
                    line = twitter_util.prepare_quote(text_file)
                    now = datetime.datetime.now()
                    logger.info("Posting on twitter")
                    twitter.statuses.update(status=line)
                    """ No more fun """

                    prv_msg = now.strftime("%Y-%m-%d %H:%M") + " Downloaded {0} tweets after {1:.4f} minutes"
                    minutes = (time.time()-last_time_notified)/60
                    twitter.direct_messages.new(user=TWITTER_LISTENER, text=prv_msg.format(total_inserted, minutes))
                    last_time_notified = time.time()
                except Exception as e:
                    logger.error("An error occurred while notifying:")
                    trace = traceback.format_exc()
                    logger.error(trace)
                    """ it is not critical, better wait and try again """
                    time.sleep(35.2)


            time_elapsed = 0

            file = config.read('config/twitter_config.cfg')
            WRITE_RATE_TMP = config.getint('Twitter_Config', 'write_rate')
            WARN_RATE_TMP = config.getint('Twitter_Config', 'warn_rate')
            if WRITE_RATE != WRITE_RATE_TMP:
                logger.info("WRITE RATE changed from {0} to {1} ".format(WRITE_RATE, WRITE_RATE_TMP))
                WRITE_RATE = WRITE_RATE_TMP
            if WARN_RATE != WARN_RATE_TMP:
                logger.info("WARN RATE changed from {0} to {1} ".format(WARN_RATE, WARN_RATE_TMP))
                WARN_RATE = WARN_RATE_TMP


except Exception as e:
    logger.error("An error occurred while downloading tweets:")
    trace = traceback.format_exc()
    logger.error(trace)

    if DM_NOTIFICATIONS:
        """ Send a DM to notify of the problem """
        logger.info("Warn your master!")
        now = datetime.datetime.now()
        error_type = "{0}]".format(e.__class__.__name__)
        error_message = "[ERROR: " + error_type + " "
        dm_text = now.strftime("%Y-%m-%d %H:%M") + error_message + "Application is shutting down after {0} tweets!"
        dm_text = dm_text.format(total_inserted)
        logger.info("Sending: " + dm_text)
        twitter.direct_messages.new(user=TWITTER_LISTENER, text=dm_text)

print "-------"
# """ Wait for jobs on the message queue to finish """
# message_queue.join()
print total_inserted
