#!/usr/bin/env python
# -*- coding: utf-8 -*-

from boto.s3.connection import S3Connection
from bson.json_util import dumps
import config
import datetime
import gzip
import logging
import json
import operator
import os
import signal
import sys
import time
import tweepy
import urllib
import zc.lockfile
import psycopg2

logging.getLogger('boto').setLevel(logging.CRITICAL)
logger = logging.getLogger('cleanr')


def health():
    try:
        f = open("/var/lock/cleanr", "r")
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
    global db
    global updated
    global inserted    
    
    try:
        lock = zc.lockfile.LockFile('/var/lock/cleanr')
    except Exception, e:
        logger.warning("another cleanr running")
        sys.exit()

    try:
        conn = psycopg2.connect("dbname=chatter user=chatter host=127.0.0.1 password=chatter")
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        db = conn.cursor()
    except Exception, e:
        logger.critical("Exception: %s" % str(e))

    logger.info("started")

    while True:

        timedif     = datetime.datetime.now() - datetime.timedelta(hours=24)
        lasthour    = timedif.strftime('%Y%m%d%H')

        logger.info("updating buckets")

	db.execute("select bucket, count(*) as c from tweets group by bucket")
	results = db.fetchall()

        for row in results:
            db.execute("select update_counts(%s,%s)", (int(row[0]), int(row[1]),))
            #logger.info("%s %s" % (row[0], row[1],))
        
        logger.info("getting bucket from %s or before" % (lasthour))

        db.execute("select bucket from bucket_history where archived = %s and bucket <= %s", (False, lasthour,))
        results = db.fetchall()

        for bucket in results:
            logger.info("processing %s" % (bucket))
            filename = config.TWITTER_FILE_PATH + config.TWITTER_FILE_PREFIX + str(bucket[0]) + '.gz'
            dumpHourToDisk(str(bucket[0]), filename)
            pushToS3()

        logger.info("all processed buckets moved. sleeping for 10 minutes.")
        time.sleep(600) 

def stop():
    
    logger.info("streamr: attempting to stop cleanr")
    
    try:
        f = open("/var/lock/cleanr", "r")
        for line in f:
            pid = line.strip()
            os.kill(int(pid), signal.SIGKILL)
            
        os.remove('/var/lock/cleanr')
        logger.info("stopped")
    
    except Exception, e:
        logger.error("Exception: %s" % str(e))


def dumpHourToDisk(hour, filename):
    global db

    db.execute("SELECT tweet from tweets where bucket = %s", (int(hour),))
    output_file = gzip.open(filename, "wb")
    x=0
    while True:        
        #from ngramr import stop as ngstop
        #ngstop()
        #zc.lockfile.LockFile('/var/lock/ngramr')

	results = db.fetchmany(1000);

	if not results:
	    break

        for record in results:
            output_file.write("%s" % str(record[0]))
            x = x + 1

    logger.info("saved %s records to %s" % (x, filename))
    output_file.close()

    logger.info("purging records from db")
    db.execute("DELETE from tweets where bucket = %s", (hour,))
    db.execute("UPDATE bucket_history SET archived=%s where bucket = %s", (True, hour,))
        #logger.info("clearing lock on ngramr")
        #os.remove('/var/lock/ngramr')


def pushToS3():

    for file in os.listdir(config.TWITTER_FILE_PATH):

        if file.endswith(".gz"):

            if os.path.isfile(config.TWITTER_FILE_PATH + '' + file):

                conn = S3Connection(config.S3AUTH, config.S3KEY)
                bucket = conn.get_bucket(config.S3BUCKET)

                key = bucket.new_key(file)
                key.set_contents_from_filename(config.TWITTER_FILE_PATH + '' + file)
                logger.info("Pushing %s to %s" % (file, config.S3BUCKET))

                logger.info("Deleting %s" % file)
                os.unlink(config.TWITTER_FILE_PATH + '' + file)

            else:
                logger.info("Nothing to push. %s is missing" % file)
