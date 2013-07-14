import re
import Queue
import HTMLParser
import dateutil.parser as parser


class TwitterData:

    #Twitter Datum properties
    tweet_fields_list = ['id', 'user_id', 'in_reply_to_status_id', 'in_reply_to_user_id', 'favorited', 'retweeted', 'retweet_count', 'lang', 'created_at']
    tweet_text_fields_list = ['tweet_id', 'user_id', 'text', 'geo_lat', 'geo_long', 'place_full_name', 'place_id']
    tweet_url_fields_list = ['tweet_id', 'user_id', 'progressive', 'url']
    tweet_hashtag_fields_list = ['tweet_id', 'user_id', 'hashtag_id']
    user_fields_list = ['id', 'screen_name', 'name', 'verified', 'protected', 'followers_count', 'friends_count', 'statuses_count', 'favourites_count', 'location', 'utc_offset', 'time_zone', 'geo_enabled', 'lang', 'description', 'url', 'created_at']

    #Utils for cleaning
    highpoints = re.compile(u'[\U00010000-\U0010ffff]')
    alphanum = re.compile(u'^[\w]+$')

    def __init__(self, buffer_size):
        #Queue of Twitter data records
        self.tweets_queue = Queue.Queue(buffer_size)
        self.tweet_texts_queue = Queue.Queue(buffer_size)
        self.users_queue = Queue.Queue(buffer_size)
        self.hashtags_queue = Queue.Queue()
        self.urls_queue = Queue.Queue()

    def contains_fields(self, data_array, fields_list, skip_list=[]):
        for field in fields_list:
            if not field in data_array and not field in skip_list:
                return False
        return True

    def parse_tweet_basic_infos(self, tweet, tweet_fields_list):
        tweet_record = []
        user_data = tweet['user']
        user_id = user_data['id']

        for field in tweet_fields_list:
                if field == 'user_id':
                    tweet_record.append(user_id)
                elif field == 'created_at':
                    datetime = parser.parse(tweet['created_at'])
                    datetime = datetime.isoformat(' ')[:-6]
                    tweet_record.append(datetime)
                elif field in tweet:
                    if not tweet[field]:
                        value = 0
                    else:
                        value = tweet[field]
                    tweet_record.append(value)
        return tweet_record

    def parse_tweet_text_infos(self, tweet, tweet_text_fields_list):
        tweet_text_record = []
        user_data = tweet['user']
        user_id = user_data['id']

        html_parser = HTMLParser.HTMLParser()

        for field in tweet_text_fields_list:
            if field == 'tweet_id':
                tweet_text_record.append(tweet['id'])
            elif field == 'user_id':
                tweet_text_record.append(user_id)
            elif field == 'text':
                if not tweet['text']:
                    value = ''
                else:
                    value = tweet['text'].strip()
                    value = self.highpoints.sub(u'', value)
                    value = html_parser.unescape(value)
                tweet_text_record.append(value)
            elif field == 'geo_lat':
                if not tweet['coordinates']:
                    tweet_text_record.append(0)
                else:
                    tweet_text_record.append(tweet['coordinates']['coordinates'][0])
            elif field == 'geo_long':
                if not tweet['coordinates']:
                    tweet_text_record.append(0)
                else:
                    tweet_text_record.append(tweet['coordinates']['coordinates'][1])
            elif field == 'place_full_name':
                if not tweet['place']:
                    value = ''
                else:
                    value = tweet['place']['full_name'].strip()
                    value = self.highpoints.sub(u'', value)
                    value = html_parser.unescape(value)
                tweet_text_record.append(value)
            elif field == 'place_id':
                # http://api.twitter.com/1/geo/id/6b9ed4869788d40e.json
                if not tweet['place']:
                    tweet_text_record.append('')
                else:
                    tweet_text_record.append(tweet['place']['id'])
            elif field in tweet:
                if not tweet[field]:
                    value = 0
                else:
                    value = tweet[field]
                tweet_text_record.append(value)

        return tweet_text_record

    def parse_user_infos(self, user_data, user_fields_list):
        user_record = []
        #user_id = user_data['id']

        html_parser = HTMLParser.HTMLParser()

        for field in user_fields_list:
            if field == 'created_at':
                datetime = parser.parse(user_data['created_at'])
                datetime = datetime.isoformat(' ')[:-6]
                user_record.append(datetime)
            elif field == 'lang':
                if not user_data['lang']:
                    value = 'NN'
                else:
                    #TODO: lang codes are longer than 2
                    value = user_data['lang'][:2]
                user_record.append(value)
            elif field == 'utc_offset':
                if not user_data['utc_offset']:
                    user_record.append(0)
                else:
                    user_record.append(user_data['utc_offset'])
            elif field == 'url':
                if not user_data['url']:
                    user_record.append('')
                else:
                    value = user_data['url'][:159]
                    user_record.append(value)
            elif field in ['description', 'name', 'location']:
                if not user_data[field]:
                    value = ''
                else:
                    value = user_data[field].strip()
                    value = self.highpoints.sub(u'', value)
                    value = html_parser.unescape(value)
                user_record.append(value)
            elif field in ['followers_count', 'friends_count', 'statuses_count', 'favourites_count']:
                value = user_data[field]
                if value is None or value < 0:
                    return None
                user_record.append(value)
            elif field in ['verified', 'protected', 'geo_enabled']:
                user_record.append(user_data[field])
            elif field in user_data:
                if not user_data[field]:
                    value = ''
                else:
                    value = user_data[field]
                user_record.append(value)
        return user_record

    def enqueue_tweet_data(self, tweet):
        tweet_record = []
        tweet_text_record = []
        user_record = []

        user_data = tweet['user']
        user_id = user_data['id']

        tweet_record = self.parse_tweet_basic_infos(tweet, self.tweet_fields_list)
        if tweet_record is None:
            #logger.error("Problem parsing tweet {0} ".format(tweet['id']))
            return False

        tweet_text_record = self.parse_tweet_text_infos(tweet, self.tweet_text_fields_list)
        if tweet_text_record is None:
            #logger.error("Problem parsing text for tweet {0} ".format(tweet['id']))
            return False

        user_record = self.parse_user_infos(user_data, self.user_fields_list)
        if user_record is None:
            # logger.info("Problem parsing user {0} for tweet {1} ".format(user_id, tweet['id']))
            return False

        #Enqueue
        self.tweets_queue.put(tweet_record)
        self.tweet_texts_queue.put(tweet_text_record)
        self.users_queue.put(user_record)

        #To avoid duplicates
        tweet_inserted_hashtags = []

        if len(tweet['entities']) > 0:
            if len(tweet['entities']['urls']) > 0:
                url_count = 0
                for url in tweet['entities']['urls']:
                    url_count = url_count + 1
                    self.urls_queue.put([tweet['id'], user_id, url_count, url['expanded_url']])

            if len(tweet['entities']['hashtags']) > 0:
                for hash in tweet['entities']['hashtags']:
                    hash_text = self.highpoints.sub(u'', hash['text'])
                    hash_text = hash_text.lower()
                    valid_hashtag = self.alphanum.match(hash_text)
                    if valid_hashtag and hash_text not in tweet_inserted_hashtags:
                        partition = ord(hash_text[0])
                        self.hashtags_queue.put([hash_text, partition, tweet['id'], user_id])
                        tweet_inserted_hashtags.append(hash_text)
        return True
