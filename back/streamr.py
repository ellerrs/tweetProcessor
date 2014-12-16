#!/usr/bin/env python
# -*- coding: utf-8 -*-

import config
import datetime
import getopt
import json
import logging
import logging.config
import os
from pymongo import MongoClient
import signal
import sys
from tendo import singleton
import tweepy
import time
import zc.lockfile


client = MongoClient('mongodb://' + config.MONGO_USER + ':' + config.MONGO_PASS + '@' + config.MONGO_HOST + '/' + config.MONGO_DB)
db = client.twitter

logger = logging.getLogger('streamr')

class StreamWatcherListener(tweepy.StreamListener):

    def on_connect(self):
        logger.info('Streaming connected and started')

    def on_data(self, data):
        insert_data = json.loads(data)
        insert_data['bucket'] = datetime.datetime.now().strftime('%Y%m%d%H')
        insert_data['timestamp'] = time.strftime('%Y%m%d%H%M%S', time.strptime(insert_data['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
        insert_data['processed'] = 0
        db.hose.insert(insert_data)

    def on_error(self, status_code):
        logger.error("Status code: %s." % status_code)
        time.sleep(3)
        return True

    def on_timeout(self):
        logger.error("Timeout.")


def start():

    try: 
        lock = zc.lockfile.LockFile('/var/lock/streamr')
        logger.info("started")

    except Exception,e:
        logger.warning("another streamr running")
        sys.exit()

    try: 
        auth = tweepy.OAuthHandler(config.TWITTER_CONSUMER_KEY, config.TWITTER_CONSUMER_SECRET)
        auth.set_access_token(config.TWITTER_ACCESS_TOKEN, config.TWITTER_ACCESS_TOKEN_SECRET)
        logger.info("Twitter auth completed")

    except Exception,e:
        logger.critical("Exception: %s" % str(e))

    try: 
        listener = StreamWatcherListener()
        stream   = tweepy.Stream(auth, listener)
        stream.filter(locations=[-125,24,-60,50])

    except Exception,e:
        logger.critical("Exception: %s" % str(e))


def stop():
    
    logger.info("attempting to stop streamr")
    
    try:
        f = open("/var/lock/streamr", "r")
        for line in f:
            pid = line.strip()
            os.kill(int(pid), signal.SIGKILL)
            
        os.remove('/var/lock/streamr')
        logger.info("stopped")
    
    except Exception, e:
        logger.error("Exception: %s" % str(e))

