# -*- coding: utf-8 -*-

import tweepy
import time
import datetime
import sys
import os
import json

from pymongo import MongoClient
from tendo import singleton

me = singleton.SingleInstance()

client = MongoClient(os.environ['MONGO_HOST'], int(os.environ['MONGO_PORT']))
client.twitter.authenticate(os.environ['MONGO_USER'], os.environ['MONGO_PASS'], mechanism='MONGODB-CR')
db = client.twitter
def log_error(msg):
    timestamp = time.strftime('%Y%m%d:%H%M:%S')
    sys.stderr.write("%s: %s\n" % (timestamp,msg))

class StreamWatcherListener(tweepy.StreamListener):
    def on_data(self, data):
        insert_data = json.loads(data)
        insert_data['bucket'] = datetime.datetime.now().strftime('%Y%m%d%H')
        obj_id = db.a.insert(insert_data)
	sys.stdout.write(data)

    def on_error(self, status_code):
      log_error("Status code: %s." % status_code)
      time.sleep(3)
      return True  # keep stream alive

    def on_timeout(self):
      log_error("Timeout.")

def main():
    consumer_key        = os.environ['TWITTER_CONSUMER_KEY']
    consumer_secret     = os.environ['TWITTER_CONSUMER_SECRET']
    access_token        = os.environ['TWITTER_ACCESS_TOKEN']
    access_token_secret = os.environ['TWITTER_ACCESS_TOKEN_SECRET']
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    listener = StreamWatcherListener()
    stream = tweepy.Stream(auth, listener)
    stream.filter(locations=[-125,24,-60,50])

if __name__ == '__main__':
    try:
      main()
    except Exception,e:
      log_error("Exception: %s" % str(e))
      time.sleep(3)

