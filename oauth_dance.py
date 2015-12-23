import os
import ConfigParser
from twitter import *

print 'Reading config file...'
config  = ConfigParser.ConfigParser()
file    = config.read('config/twitter_config.cfg')


CREDS_FILE          = config.get('Twitter_Config', 'twitter_creds')
TWITTER_USERNAME    = config.get('Twitter_Config', 'username')
CONSUMER_KEY        = config.get('Twitter_Config', 'consumer_key')
CONSUMER_SECRET     = config.get('Twitter_Config', 'consumer_secret')
TWITTER_CREDS       = os.path.expanduser(CREDS_FILE)

if os.path.exists(TWITTER_CREDS):
    print "Credential file already exists!"
else :
    oauth_dance(TWITTER_USERNAME, CONSUMER_KEY,  CONSUMER_SECRET, TWITTER_CREDS)


