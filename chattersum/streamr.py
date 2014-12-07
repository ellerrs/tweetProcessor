# -*- coding: utf-8 -*-

import config
import datetime
import getopt
import json
import logging
import os
from pymongo import MongoClient
import signal
import sys
from tendo import singleton
import tweepy
import time
import zc.lockfile


client = MongoClient( config.MONGO_HOST, config.MONGO_PORT)
client.twitter.authenticate( config.MONGO_USER, config.MONGO_PASS, mechanism='MONGODB-CR')
db = client.twitter


class StreamWatcherListener(tweepy.StreamListener):

    def on_connect(self):
        logging.info('Streaming connected and started')

    def on_data(self, data):
        insert_data = json.loads(data)
        insert_data['bucket'] = datetime.datetime.now().strftime('%Y%m%d%H')
        insert_data['timestamp'] = time.strftime('%Y%m%d%H%M%S', time.strptime(insert_data['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
        insert_data['processed'] = False
        db.hose.insert(insert_data)

    def on_error(self, status_code):
        logging.error("Status code: %s." % status_code)
        time.sleep(3)
        return True

    def on_timeout(self):
        logging.error("Timeout.")


def start():

    lock = zc.lockfile.LockFile('/var/lock/streamr')
    logging.info("streamr started")

    try: 
        auth = tweepy.OAuthHandler(config.TWITTER_CONSUMER_KEY, config.TWITTER_CONSUMER_SECRET)
        auth.set_access_token(config.TWITTER_ACCESS_TOKEN, config.TWITTER_ACCESS_TOKEN_SECRET)
        logging.info("Twitter auth completed")

    except Exception,e:
        logging.critical("Exception: %s" % str(e))

    try: 
        listener = StreamWatcherListener()
        stream   = tweepy.Stream(auth, listener)
        stream.filter(locations=[-125,24,-60,50])

    except Exception,e:
        logging.critical("Exception: %s" % str(e))


def stop():
    
    logging.info("attempting to stop streamr")
    
    try:
        f = open("/var/lock/streamr", "r")
        for line in f:
            pid = line.strip()
            os.kill(int(pid), signal.SIGKILL)
            
        os.remove('/var/lock/streamr')
        logging.info("streamr stopped")
    
    except Exception, e:
        logging.error("Exception: %s" % str(e))

