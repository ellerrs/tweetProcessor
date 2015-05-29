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
        zc.lockfile.LockFile('/var/lock/cleanr')
        logger.info("started")
    except:
        logger.warning("another cleanr running")
        sys.exit()

    try:
        conn = psycopg2.connect("dbname=chatter user=chatter host=127.0.0.1 password=chatter")
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        db = conn.cursor()
    except Exception, e:
        logger.critical("Exception: %s" % str(e))


    while True:

        timedif     = datetime.datetime.now() - datetime.timedelta(hours=24)
        lasthour    = timedif.strftime('%Y%m%d%H')
        
        db.execute("SELECT tweet from tweets where bucket = %s limit 10", (lasthour,))
        for record in db:
            print record


        # for x in db.buckets.find({}).sort([('_id', 1)]):
        #     if ((x['unprocessed'] == 0) and (x['_id'] <> lasthour )):
        #         logger.info("bucket %s ready " % x['_id'])

        #         from ngramr import stop as ngstop
        #         ngstop()
        #         zc.lockfile.LockFile('/var/lock/ngramr')
 
        #         filename = config.TWITTER_FILE_PATH + config.TWITTER_FILE_PREFIX + x['_id'] + '.txt.gz'
        #         s3filename = config.TWITTER_FILE_PREFIX + x['_id'] + '.txt.gz'
        #         dumpHourToDisk(x['_id'], filename)
        #         pushToS3()

        #         logger.info("clearing lock on ngramr")
        #         os.remove('/var/lock/ngramr')


        #     else:
        #         logger.info("bucket %s not ready. %s unprocessed" % (x['_id'], x['unprocessed']))

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
    
    records = db.hose.find({'bucket':hour}).count()
    
    if records == 0:
        logger.info("No records found. Not creating a file for %s" % hour)
    else:
        logger.info("Found %s records. Creating file: %s" % (records, filename))
        systemCall = "mongoexport --quiet --username " + config.MONGO_USER + " --password " + config.MONGO_PASS + " --authenticationDatabase " + config.MONGO_DB + " --db " + config.MONGO_DB + " --collection hose --query '{\"bucket\":\"" + hour + "\"}' | gzip > " + filename
        os.system(systemCall)
        db.hose.remove({'bucket': hour})
        logger.info("saved %s records to %s" % (records, filename))


def pushToS3():

    for file in os.listdir(config.TWITTER_FILE_PATH):

        if file.endswith(".txt.gz"):

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


def purgeDB(hour):

    records = db.hose.find({'bucket':hour}).count();
    logger.info("removing bucket: %s. Found %s records" % (hour, records))
    db.hose.remove({'bucket': hour})

