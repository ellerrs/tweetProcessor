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
from pymongo import MongoClient
import signal
import sys
import time
import tweepy
import urllib
import zc.lockfile

client = MongoClient('mongodb://' + config.MONGO_USER + ':' + config.MONGO_PASS + '@' + config.MONGO_HOST + '/' + config.MONGO_DB)
db = client.twitter 

logger = logging.getLogger('cleanr')

def start():
    global updated
    global inserted    

    try:
        lock = zc.lockfile.LockFile('/var/lock/cleanr')
        logger.info("started")

    except Exception,e:
        logger.warning("another cleanr running")
        sys.exit()

    pushToS3()

    while True:

        timedif     = datetime.datetime.now() - datetime.timedelta(hours=0)
        lasthour    = timedif.strftime('%Y%m%d%H')
        logger.info("updating bucket list")
        db.hose.aggregate([{"$group":{"_id":'$bucket',"processed":{"$sum":{"$cond":[{"$eq":['$processed',1]},1,0]}},"unprocessed":{"$sum":{"$cond":[{"$eq":['$processed',0]},1,0]}}}}, {"$out": "buckets"}])
        
        for x in db.buckets.find({}).sort([('_id', 1)]):
            if ((x['unprocessed'] == 0) and (x['_id'] <> lasthour )):
                logger.info("bucket %s ready " % x['_id'])

                filename = config.TWITTER_FILE_PATH + config.TWITTER_FILE_PREFIX + x['_id'] + '.txt.gz'
                s3filename = config.TWITTER_FILE_PREFIX + x['_id'] + '.txt.gz'
                dumpHourToDisk(x['_id'], filename)
                pushToS3()

            else:
                logger.info("bucket %s not ready. %s unprocessed" % (x['_id'], x['unprocessed']))

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
        output  = gzip.open( config.TWITTER_FILE_PATH + 'temp' , 'wb')
        while (db.hose.find({'bucket':hour}).count() > 0):
            for x in db.hose.find({'bucket':hour}).limit(5000):
                output.write(dumps(x))
                output.write('\n')
                db.hose.remove({'_id': x['_id']})
            
            logger.info("Saved 5000 to file")
            time.sleep(10)

        output.close()

        os.rename( config.TWITTER_FILE_PATH + 'temp', config.TWITTER_FILE_PATH + '' + filename)
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

