import os
import sys
import logging
import random
import datetime
import dateutil
import ConfigParser
import MySQLdb
import pprint
"""  check libraries """

from twitter import *
from twitter_helper import util as twitter_util

""" This code is intentionally importing any library used by the library """

config = ConfigParser.ConfigParser()
file = config.read('config/twitter_config.cfg')

CREDS_FILE          = config.get('Twitter_Config', 'twitter_creds')
TWITTER_USERNAME    = config.get('Twitter_Config', 'username')
CONSUMER_KEY        = config.get('Twitter_Config', 'consumer_key')
CONSUMER_SECRET     = config.get('Twitter_Config', 'consumer_secret')
TWITTER_CREDS       = os.path.expanduser(CREDS_FILE)

oauth_token, oauth_secret = read_token_file(TWITTER_CREDS)
oauth = OAuth( oauth_token, oauth_secret,CONSUMER_KEY,  CONSUMER_SECRET)

#Connecting to Twitter
print "Connecting to the stream..."

#twitter = TwitterStream(domain='stream.twitter.com', auth=UserPassAuth('EsampleUsername', 'EsamplePWDxxx')) <-- no more active
twitter = TwitterStream(domain='stream.twitter.com', auth=oauth, block=True)

print "Opening the stream..."

#ver = twitter.account.verify()
#print ver
iterator = twitter.statuses.filter(track="love")
# Different test
#iterator = twitter.user()

print "Reading the stream..."

#iterator = twitter.statuses.home_timeline()

now = datetime.datetime.now()
print now.strftime("%Y-%m-%d %H:%M")

count = 0
for tweet in iterator :
        # print tweet
        if tweet is not None and 'text' in tweet:
                print tweet['id_str']
                print tweet['id']
                print tweet['is_quote_status']
                print "\n twt:"
                for key, value in tweet.iteritems() :
                       print key
                
                count = count + 1
                if count > 1 :
                    break

        elif tweet is not None :
                print "\n SPECIAL:"
                for key in tweet.iteritems() :
                       print key


print "Downloaded {0}".format(count)
print "Done!"
