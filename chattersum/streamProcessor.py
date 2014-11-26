# -*- coding: utf-8 -*-

import tweepy
import time
import datetime
import sys
import os
import json
import config
from tendo import singleton
from pymongo import MongoClient

client = MongoClient( config.MONGO_HOST, config.MONGO_PORT)
client.twitter.authenticate( config.MONGO_USER, config.MONGO_PASS, mechanism='MONGODB-CR')
db = client.twitter


def log_error(msg):
    timestamp = time.strftime('%Y%m%d:%H%M:%S')
    sys.stderr.write("%s: %s\n" % (timestamp,msg))

class StreamWatcherListener(tweepy.StreamListener):
    def on_data(self, data):
        insert_data = json.loads(data)
        insert_data['bucket'] = datetime.datetime.now().strftime('%Y%m%d%H')
        obj_id = db.a.insert(insert_data)

    def on_error(self, status_code):
      log_error("Status code: %s." % status_code)
      time.sleep(3)
      return True  # keep stream alive

    def on_timeout(self):
      log_error("Timeout.")


def main():

    me = singleton.SingleInstance()
    
    auth = tweepy.OAuthHandler(config.TWITTER_CONSUMER_KEY, config.TWITTER_CONSUMER_SECRET)
    auth.set_access_token(config.TWITTER_ACCESS_TOKEN, config.TWITTER_ACCESS_TOKEN_SECRET)

    listener 	= StreamWatcherListener()
    stream 	= tweepy.Stream(auth, listener)
    stream.filter(locations=[-125,24,-60,50])

if __name__ == '__main__':
    try:
      main()
    except Exception,e:
      log_error("Exception: %s" % str(e))
      time.sleep(3)

