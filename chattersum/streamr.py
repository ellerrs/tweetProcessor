# -*- coding: utf-8 -*-

import config
import datetime
import getopt
import json
import os
from pymongo import MongoClient
import signal
import subprocess
import sys
from tendo import singleton
import tweepy
import time
import zc.lockfile

#import ngramr


client = MongoClient( config.MONGO_HOST, config.MONGO_PORT)
client.twitter.authenticate( config.MONGO_USER, config.MONGO_PASS, mechanism='MONGODB-CR')
db = client.twitter


def log_error(msg):
    os.remove('/var/lock/streamr')
    timestamp = time.strftime('%Y%m%d:%H%M:%S')
    sys.stderr.write("%s: %s\n" % (timestamp,msg))

def log_info(msg):
    timestamp = time.strftime('%Y%m%d:%H%M:%S')
    sys.stdout.write("%s: %s\n" % (timestamp,msg))


class StreamWatcherListener(tweepy.StreamListener):

    def on_connect(self):
        log_info("Streaming connected and started")

    def on_data(self, data):
        insert_data = json.loads(data)
        insert_data['bucket'] = datetime.datetime.now().strftime('%Y%m%d%H')
        insert_data['timestamp'] = time.strftime('%Y%m%d%H%M%S', time.strptime(insert_data['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
        insert_data['processed'] = False
        db.hose.insert(insert_data)
        #ngramr.buildGramsStream(insert_data)

    def on_error(self, status_code):
        log_error("Status code: %s." % status_code)
        time.sleep(3)
        return True

    def on_timeout(self):
        log_error("Timeout.")


def start():

    lock = zc.lockfile.LockFile('/var/lock/streamr')
    log_info("streamr started")

    try: 
        auth = tweepy.OAuthHandler(config.TWITTER_CONSUMER_KEY, config.TWITTER_CONSUMER_SECRET)
        auth.set_access_token(config.TWITTER_ACCESS_TOKEN, config.TWITTER_ACCESS_TOKEN_SECRET)
        log_info("Twitter auth completed")
    except Exception,e:
        log_error("Exception: %s" % str(e))

    try: 
        listener = StreamWatcherListener()
        stream   = tweepy.Stream(auth, listener)
        stream.filter(locations=[-125,24,-60,50])
    except Exception,e:
        log_error("Exception: %s" % str(e))

def stop():
    log_info("attempting to stop streamr")
    try:
        f = open("/var/lock/streamr", "r")
        for line in f:
            pid = line.strip()
            os.kill(int(pid), signal.SIGKILL)
            
        os.remove('/var/lock/streamr')
        log_info("streamr stopped")
    except Exception, e:
        log_error("Exception: %s" % str(e))