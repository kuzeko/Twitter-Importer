import gc
import os
import sys
import logging
import ConfigParser
import MySQLdb

from twitter import *


config = ConfigParser.ConfigParser()
file = config.read('config/twitter_config.cfg')

CREDS_FILE          = config.get('Twitter_Config', 'twitter_creds')
TWITTER_USERNAME    = config.get('Twitter_Config', 'username')
CONSUMER_KEY        = config.get('Twitter_Config', 'consumer_key')
CONSUMER_SECRET     = config.get('Twitter_Config', 'consumer_secret')
TWITTER_CREDS       = os.path.expanduser(CREDS_FILE)

oauth_token, oauth_secret = read_token_file(TWITTER_CREDS)
oauth = OAuth( oauth_token, oauth_secret,CONSUMER_KEY,  CONSUMER_SECRET)

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
