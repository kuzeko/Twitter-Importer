import re
import HTMLParser
import dateutil.parser as parser


def contains_fields(data_array, fields_list ):
    response = True
    for field in fields_list :
        response = response and (field in data_array)
    return response

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
                if not tweet[field] :
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
            if not tweet['text'] :
                value = ''
            else :
                value = tweet['text'].strip()
                value = highpoints.sub(u'', value)
                value = html_parser.unescape(value)
            tweet_text_record.append(value)
        elif field == 'geo_lat' :
            if not tweet['geo'] :
                tweet_text_record.append(0)
            else :
                tweet_text_record.append(tweet['geo']['coordinates'][0])                
        elif field == 'geo_long' :
            if not tweet['geo'] :
                tweet_text_record.append(0)
            else :
                tweet_text_record.append(tweet['geo']['coordinates'][1])                
        elif field == 'place_full_name' :
            if not tweet['place'] :
                tweet_text_record.append('')
            else :
                tweet_text_record.append(tweet['place']['full_name'])                
        elif field == 'place_id' :
            # http://api.twitter.com/1/geo/id/6b9ed4869788d40e.json
            if not tweet['place'] :
                tweet_text_record.append('')
            else :
                tweet_text_record.append(tweet['place']['id'])                
        elif field in tweet :
            if not tweet[field] :
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
            if not user_data['lang'] :
                value = 'NN'
            else :         
                value = user_data['lang'][:2]                
            user_record.append(value)
        elif field == 'utc_offset' :
            if not user_data['utc_offset']:
                user_record.append(0)
            else :
                user_record.append(user_data['utc_offset'])
        elif field == 'url' :
            if not user_data['url'] :
                user_record.append('')
            else :
                value = user_data['url'][:159]                
                user_record.append(value)            
        elif field in ['description', 'name', 'location'] :
            if not user_data[field] :
                value = ''
            else :
                value = user_data[field].strip()
                value = highpoints.sub(u'', value)
                value = html_parser.unescape(value)
            user_record.append(value)
        elif field in ['followers_count', 'friends_count', 'statuses_count', 'favourites_count'] :
            value = user_data[field]
            if value == None or value < 0 :
                return None
            user_record.append(value)            
        elif field in ['verified', 'protected', 'geo_enabled'] :
            user_record.append(user_data[field])                        
        elif field in user_data :
            if not user_data[field] :
                value = ''
            else :
                value = user_data[field]
            user_record.append(value)
    return user_record




