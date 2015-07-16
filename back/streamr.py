#!/usr/bin/env python
# -*- coding: utf-8 -*-

import config
import datetime
import getopt
import json
import logging
import logging.config
import os
import signal
import sys
import tweepy
import time
import zc.lockfile
import psycopg2

logger = logging.getLogger('streamr')

class StreamWatcherListener(tweepy.StreamListener):

    def on_connect(self):
        global db
        logger.info('Streaming connected and started')
        try:
            conn = psycopg2.connect("dbname=chatter user=chatter host=127.0.0.1 password=chatter")
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            db = conn.cursor()
        except Exception, e:
            logger.critical("Exception: %s" % str(e))

    def on_data(self, data):
        global db
        insert_data = json.loads(data)
        if 'created_at' in insert_data:
	    bucket    = datetime.datetime.now().strftime('%Y%m%d%H')
	    dt        = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(insert_data['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))

            try:
                db.execute("INSERT INTO tweets (tweet, bucket, created) VALUES (%s, %s, %s)",(data,bucket,dt))
            except Exception, e:
		logger.critical("Exception: %s" % str(e))
                stop()
                time.sleep(10)
                start()
	else:
            pass

    def on_error(self, status_code):
        logger.error("Status code: %s." % status_code)
        time.sleep(3)
        return True

    def on_timeout(self):
        logger.error("Timeout.")


def health():
    try:
        f = open("/var/lock/streamr", "r")
        for line in f:
            pid = line.strip()
            try:
                os.kill(int(pid), 0)
            except OSError:
                return False
            else:
                return True
    except:
        return False


def start():

    try: 
        lock = zc.lockfile.LockFile('/var/lock/streamr')
    except:
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
        time.sleep(10)
        start()


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

