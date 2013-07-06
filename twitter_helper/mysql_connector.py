import MySQLdb
import warnings
import Queue
import traceback
from time import time
from collections import deque


class MysqlTwitterConnector:
    """ Inserts Tweet records into a MySQL Database """

    @staticmethod
    def test(host, user, passwd, db, charset='utf8'):
        conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset=charset)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*), VERSION() from tweet")
        ver = cursor.fetchone()
        cursor.close()
        conn.close()
        return ver is not None

    @staticmethod
    def get_all_elements(queue):
        queue_copy = []
        while True:
            try:
                record = queue.get(block=False)
            except Queue.Empty:
                break
            else:
                queue_copy.append(record)
        return queue_copy

    def __init__(self, host, user, passwd, db, charset='utf8', data_parser=None, cache_size=1):
        self.hashtag_buffer = deque(maxlen=cache_size)
        self.cache_size = cache_size
        """ Connect to the  Database """
        self.conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset=charset)
        self.cursor = self.conn.cursor()

        """ Creates the insert and updated queries for the database """
        tweet_fields = ', '.join(data_parser.tweet_fields_list)
        tweet_placeholders = ', '.join(['%s']*len(data_parser.tweet_fields_list))
        self.insert_tweets_sql = 'REPLACE INTO tweet (' + tweet_fields + ') VALUES (' + tweet_placeholders + ')'

        tweet_text_fields = ', '.join(data_parser.tweet_text_fields_list)
        tweet_text_placeholders = ', '.join(['%s']*len(data_parser.tweet_text_fields_list))
        self.insert_tweets_texts_sql = 'REPLACE INTO tweet_text (' + tweet_text_fields + ') VALUES (' + tweet_text_placeholders + ')'

        tweet_url_fields = ', '.join(data_parser.tweet_url_fields_list)
        tweet_url_placeholders = ', '.join(['%s']*len(data_parser.tweet_url_fields_list))
        self.insert_tweets_urls_sql = 'INSERT INTO tweet_url (' + tweet_url_fields + ') VALUES ( ' + tweet_url_placeholders + ') ON DUPLICATE KEY UPDATE tweet_id=VALUES(tweet_id)'

        tweet_hashtag_fields = ', '.join(data_parser.tweet_hashtag_fields_list)
        tweet_hashtag_placeholders = ', '.join(['%s']*len(data_parser.tweet_hashtag_fields_list))
        self.insert_tweets_hashtags_sql = 'REPLACE INTO tweet_hashtag (' + tweet_hashtag_fields + ') VALUES (' + tweet_hashtag_placeholders + ')'

        self.insert_hashtags_sql = 'INSERT INTO hashtag (hashtag, partitioning_value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE hashtag=VALUES(hashtag), partitioning_value=VALUES(partitioning_value)'

        user_fields = ', '.join(data_parser.user_fields_list)
        user_placeholders = ', '.join(['%s']*len(data_parser.user_fields_list))
        self.insert_users_sql = 'REPLACE INTO user (' + user_fields + ') VALUES (' + user_placeholders + ') '

    def insert_records(self, tweets_queue, tweet_texts_queue, users_queue, urls_queue, hashtags_queue, message_queue):
        try:
            """ Catch  MySQL errors/warnings """
            warnings.filterwarnings('error', category=MySQLdb.Warning)

            # missing_count = len(missing_users)
            # for user_id in missing_users:
            #     if user_id not in users:
            #         missing_count = missing_count - 1
            # logger.info("Missing {0} users ".format(missing_count))
            #logger.info("Inserting {0} tweets and {1} texts ".format(len(tweets), len(tweet_texts)))
            time_start = time()

            tweets = MysqlTwitterConnector.get_all_elements(tweets_queue)
            self.cursor.executemany(self.insert_tweets_sql, tweets)
            tweet_texts = MysqlTwitterConnector.get_all_elements(tweet_texts_queue)
            self.cursor.executemany(self.insert_tweets_texts_sql, tweet_texts)
            message_queue.put((1, "Inserted {0} tweets and {1} texts in {2} sec".format(len(tweets), len(tweet_texts), time()-time_start)))

            #logger.info("Inserting {0} tweet urls ".format(len(urls)))
            urls = MysqlTwitterConnector.get_all_elements(urls_queue)
            self.cursor.executemany(self.insert_tweets_urls_sql, urls)

            #logger.info("Inserting {0} tweet hashtags ".format(len(hashtags)))
            hashtags = MysqlTwitterConnector.get_all_elements(hashtags_queue)
            tweet_hashtags = self.insert_hashtags(hashtags)
            self.cursor.executemany(self.insert_tweets_hashtags_sql, tweet_hashtags)

            users = MysqlTwitterConnector.get_all_elements(users_queue)
            #logger.info("Inserting {0} users ".format(len(list_users)))
            self.cursor.executemany(self.insert_users_sql, users)

            message_queue.put((1, "Commit..."))
            self.conn.commit()

            time_elapsed = (time() - time_start)
            message_queue.put((1, "Queries executed in {0} seconds ".format(time_elapsed)))
        except Exception:
            error_message = "An error occurred while executing the query:\n"
            if hasattr(self.cursor, '_last_executed'):
                error_message += self.cursor._last_executed
            trace = traceback.format_exc()
            message_queue.put((-1, error_message + "\n" + trace))
            self.conn.rollback();
        finally:
            self.close()

    def insert_hashtags(self, hashtags):
        inserted_hashtags = {}
        parsed_hashtags = []
        for hashtag in hashtags:
            # [hash_text, partition, tweet['id'], user_id]
            hash_text = hashtag[0]
            partition = hashtag[1]

            if not hash_text in inserted_hashtags:
                self.cursor.execute(self.insert_hashtags_sql, [hash_text, partition])
                self.conn.commit()
                hash_id = self.cursor.lastrowid

                if hash_id is None or hash_id == 0:
                    #Order is inverted as MySQL is not so good in deciding which check do first
                    self.cursor.execute("SELECT id FROM hashtag h WHERE h.partitioning_value =%s AND h.hashtag = %s", [partition, hash_text])
                    hash_id = self.cursor.fetchone()[0]
                    #Again
                    if hash_id is None or hash_id == 0:
                        raise Exception("hash_id is {0} for {1} ".format(hash_id, hash_text))

                if len(self.hashtag_buffer) >= self.cache_size:
                    to_remove = self.hashtag_buffer.popleft()
                    del inserted_hashtags[to_remove]

                self.hashtag_buffer.append(hash_text)
                inserted_hashtags[hash_text] = hash_id
            else:
                hash_id = inserted_hashtags[hash_text]
            record = []
            # 'tweet_id', 'user_id', 'hashtag_id'
            record.append(hashtag[2], hashtag[3], hash_id)
            parsed_hashtags.append(record)
        return parsed_hashtags

    def close(self):
        self.cursor.close()
        self.conn.close()
