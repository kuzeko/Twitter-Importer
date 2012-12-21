import re
import HTMLParser
import dateutil.parser as parser

def parse_tweet_basic_infos(tweet, tweet_fields_list ):
    tweet_record = []
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
    return tweet_record


def parse_tweet_text_infos(tweet, tweet_text_fields_list ):
    tweet_text_record = []
    user_data = tweet['user']
    user_id = user_data['id']
    
    highpoints = re.compile(u'[\U00010000-\U0010ffff]')
    html_parser = HTMLParser.HTMLParser()

    
    for field in tweet_text_fields_list :
        if field == 'tweet_id' :
            tweet_text_record.append(tweet['id'])
        elif field == 'user_id' :
            tweet_text_record.append(user_id)
        elif field == 'text' :
            value = tweet['text'].strip()
            value = highpoints.sub(u'', value)
            value = html_parser.unescape(value)
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
            
    return tweet_text_record


def parse_user_infos(user_data, user_fields_list ):
    user_record = []
    user_id = user_data['id']
    
    highpoints = re.compile(u'[\U00010000-\U0010ffff]')
    html_parser = HTMLParser.HTMLParser()
    
    for field in user_fields_list :
        if field == 'created_at' :
            datetime = parser.parse(user_data['created_at'])
            datetime = datetime.isoformat(' ')[:-6]
            user_record.append(datetime)
        elif field == 'lang' :    
            value = user_data['lang'][:2]                
            user_record.append(value)
        elif field == 'utc_offset' :
            if user_data['utc_offset'] == None or  user_data['utc_offset'] == '':
                user_record.append(0)
            else :
                user_record.append(user_data['utc_offset'])
        elif field == 'url' :
            value = user_data['url'][:159]                
            user_record.append(value)
        elif field in ['description', 'name', 'location'] :
            value = user_data[field].strip()
            value = highpoints.sub(u'', value)
            value = html_parser.unescape(value)
            user_record.append(value)
        elif field in ['followers_count', 'friends_count', 'statuses_count', 'favourites_count'] :
            value = user_data[field]
            if value < 0 :
                return None
            user_record.append(value)            
        elif field in user_data :
            if user_data[field] == None :
                value = ''
            else :
                value = user_data[field]
            user_record.append(value)
    return user_record




