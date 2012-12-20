import gc
import os
import sys
import logging
import random
import datetime
import ConfigParser
import MySQLdb

from twitter import *

def random_line(afile):
    line = next(afile)
    for num, aline in enumerate(afile):
        aline = aline.strip()
        if (len(aline) < 10 or aline[0].islower() or len(aline) > 123) or random.randrange(num + 2):
            continue
        line = aline
    return line


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
line = random_line(text_file)
#print line

now = datetime.datetime.now()
print now.strftime("%Y-%m-%d %H:%M")

number = random.randrange(1,1000,2)
print number

line = "%d] " + line
line = line % number
print line

twitter.statuses.update(status=line + " -- Hamlet")
print "-------"
twitter.direct_messages.new(user="kuzeko",text="I think yer swell!")
