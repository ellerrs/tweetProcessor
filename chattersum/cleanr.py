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

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('cleanr')

#db.hose.aggregate({$group:{_id:'$bucket',processed:{$sum:{$cond:[{$eq:['$processed',1]},1,0]}},unprocessed:{$sum:{$cond:[{$eq:['$processed',0]},1,0]}}}});

def start():
    global updated
    global inserted    
    # Putting a lock file here to block hourly processing that has gone more than an hour

    lock = zc.lockfile.LockFile('/var/lock/cleanr')
    logger.info("started")

    while True:

        timedif     = datetime.datetime.now() - datetime.timedelta(hours=0)
        lasthour    = timedif.strftime('%Y%m%d%H')
        logger.info("updating bucket list")
        db.hose.aggregate([{"$group":{"_id":'$bucket',"processed":{"$sum":{"$cond":[{"$eq":['$processed',1]},1,0]}},"unprocessed":{"$sum":{"$cond":[{"$eq":['$processed',0]},1,0]}}}}, {"$out": "buckets"}])
        
        for x in db.buckets.find({}).sort([('_id', 1)]):
            if ((x['unprocessed'] == 0) and (x['_id'] <> lasthour )):
                logger.info("bucket %s ready " % x['_id'])

                filename = config.TWITTER_FILE_PATH + config.TWITTER_FILE_PREFIX + x['_id'] + '.txt.gz'

                dumpHourToDisk(x['_id'], filename)
                purgeDB(x['_id'])

                pushToS3(filename)
                purgeDisk(filename)

                #purgeSingletons(x['_id'])
                
            else:
                logger.info("bucket %s not ready. %s unprocessed" % (x['_id'], x['unprocessed']))

        #    dumpHourToDisk(x['_id'])


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


def purgeSingletons(hour):

    hour_to_purge = int((int(hour) * 10000) + 100)
    logger.info("ngramr: purging singleton grams prior to %s" % hour_to_purge )
    bulkProcess = db.grams.initialize_ordered_bulk_op()
    bulkProcess.find( { "$and": [ { "lastSeen": { "$lt": hour_to_purge } }, { "high": 1 } ] } ).remove()
    results = bulkProcess.execute()
    logger.info("ngramr: purged %s grams" % results['nRemoved'])


def dumpHourToDisk(hour, filename):
    
    records = db.hose.find({'bucket':hour}).count();
    
    if records == 0:
        logger.info("No records found. Not creating a file for %s" % hour)
    else:
        logger.info("Found %s records. Creating file: %s" % (records, filename))
        output  = gzip.open( filename, 'wb')
        
        for x in db.hose.find({'bucket':hour}):
            output.write(dumps(x))
            output.write('\n')
        output.close()
        logger.info("saved %s records to %s" % (records, filename))


def purgeDisk(filename):

    if os.path.isfile(filename):
        logger.info("Deleting %s" % filename)
        os.unlink(filename)
    else:
        logger.info("%s already deleted" % filename)


def pushToS3(filename):

    if os.path.isfile(filename):
        conn = S3Connection(config.S3AUTH, config.S3KEY)
        bucket = conn.get_bucket(config.S3BUCKET)

        key = bucket.new_key(filename)
        key.set_contents_from_filename(filename)
        logger.info("Pushing %s to %s" % (filename, config.S3BUCKET))

    else:
        logger.info("Nothing to push. %s is missing" % filename)


def purgeDB(hour):

    records = db.hose.find({'bucket':hour}).count();
    logger.info("removing bucket: %s. Found %s records" % (hour, records))
    db.hose.remove({'bucket': hour})

