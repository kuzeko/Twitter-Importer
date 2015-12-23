import gc
import os
import sys
import logging
import random
import datetime
import ConfigParser
import MySQLdb

from twitter import *
from twitter_helper import util as twitter_util



config = ConfigParser.ConfigParser()
file = config.read('config/twitter_config.cfg')

CREDS_FILE          = config.get('Twitter_Config', 'twitter_creds')
TWITTER_USERNAME    = config.get('Twitter_Config', 'username')
CONSUMER_KEY        = config.get('Twitter_Config', 'consumer_key')
CONSUMER_SECRET     = config.get('Twitter_Config', 'consumer_secret')
TWITTER_CREDS       = os.path.expanduser(CREDS_FILE)

oauth_token, oauth_secret = read_token_file(TWITTER_CREDS)
oauth = OAuth( oauth_token, oauth_secret,CONSUMER_KEY,  CONSUMER_SECRET)


twitter = Twitter(auth=oauth)

text_file  = open("./Hamlet.txt")

now = datetime.datetime.now()
print now.strftime("%Y-%m-%d %H:%M")

line = twitter_util.prepare_quote(text_file)
print line

twitter.statuses.update(status=line)
print "-------"
twitter.direct_messages.new(user="kuzeko",text=now.strftime("%Y-%m-%d %H:%M") + " tweeting reached ")
