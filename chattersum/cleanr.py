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
import zc.lockfile

client = MongoClient( config.MONGO_HOST, config.MONGO_PORT)
client.twitter.authenticate( config.MONGO_USER, config.MONGO_PASS, mechanism='MONGODB-CR')
db = client.twitter

#db.hose.aggregate({$group:{_id:'$bucket',processed:{$sum:{$cond:[{$eq:['$processed',1]},1,0]}},unprocessed:{$sum:{$cond:[{$eq:['$processed',0]},1,0]}}}});

def start():
    global updated
    global inserted    
    # Putting a lock file here to block hourly processing that has gone more than an hour

    lock = zc.lockfile.LockFile('/var/lock/cleanr')
    logging.info("cleanr: started")

    while True:

        timedif     = datetime.datetime.now() - datetime.timedelta(hours=0)
        lasthour    = timedif.strftime('%Y%m%d%H')

        result = db.hose.aggregate({"$group":{"_id":'$bucket',"processed":{"$sum":{"$cond":[{"$eq":['$processed',1]},1,0]}},"unprocessed":{"$sum":{"$cond":[{"$eq":['$processed',0]},1,0]}}}})
        for x in result['result']:
            if ((x['unprocessed'] == 0) and (x['_id'] <> lasthour )):
                logging.info("cleanr: bucket %s ready " % x['_id'])

                filename = config.TWITTER_FILE_PATH + config.TWITTER_FILE_PREFIX + x['_id'] + '.txt.gz'

                dumpHourToDisk(x['_id'], filename)
                purgeDB(x['_id'])

                pushToS3(filename)
                purgeDisk(filename)

            else:
                logging.info("cleanr: bucket %s not ready. %s unprocessed" % (x['_id'], x['unprocessed']))

        #    dumpHourToDisk(x['_id'])


        logging.info("cleanr: all processed buckets moved. sleeping for 15 minutes.")
        time.sleep(900) 

def stop():
    
    logging.info("streamr: attempting to stop cleanr")
    
    try:
        f = open("/var/lock/cleanr", "r")
        for line in f:
            pid = line.strip()
            os.kill(int(pid), signal.SIGKILL)
            
        os.remove('/var/lock/cleanr')
        logging.info("cleanr: stopped")
    
    except Exception, e:
        logging.error("cleanr: Exception: %s" % str(e))


def dumpHourToDisk(hour, filename):
    
    records = db.hose.find({'bucket':hour}).count();
    
    if records == 0:
        logging.info("cleanr: No records found. Not creating a file for %s" % hour)
    else:
        logging.info("cleanr: Found %s records. Creating file: %s" % (records, filename))
        output  = gzip.open( filename, 'wb')
        
        for x in db.hose.find({'bucket':hour}):
            output.write(dumps(x))
            output.write('\n')
        output.close()
        logging.info("cleanr: saved %s records to %s" % (records, filename))


def purgeDisk(filename):

    if os.path.isfile(filename):
        logging.info("cleanr: Deleting %s" % filename)
        os.unlink(filename)
    else:
        logging.info("cleanr: %s already deleted" % filename)


def pushToS3(filename):

    if os.path.isfile(filename):
        conn = S3Connection(config.S3AUTH, config.S3KEY)
        bucket = conn.get_bucket(config.S3BUCKET)

        key = bucket.new_key(filename)
        key.set_contents_from_filename(filename)
        logging.info("cleanr: Pushing %s to %s" % (filename, config.S3BUCKET))

    else:
        logging.info("cleanr: Nothing to push. %s is missing" % filename)


def purgeDB(hour):

    records = db.hose.find({'bucket':hour}).count();
    logging.info("cleanr: removing bucket: %s. Found %s records" % (hour, records))
    db.hose.remove({'bucket': hour})

