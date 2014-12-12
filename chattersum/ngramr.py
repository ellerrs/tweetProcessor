#!/usr/bin/env python
# -*- coding: utf-8 -*-

import config
import datetime
import getopt
import logging
import logging.config
import nltk
from nltk.corpus import stopwords
from nltk.util import ngrams
import operator
import os
from pymongo import MongoClient
import signal
import sys
import time
import zc.lockfile


client = MongoClient( config.MONGO_HOST, config.MONGO_PORT)
client.twitter.authenticate( config.MONGO_USER, config.MONGO_PASS, mechanism='MONGODB-CR')
db = client.twitter

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('ngramr')

updated     = 0
inserted    = 0

#words = nltk.word_tokenize(text)

def buildGramsStream(tweet):
    
    text        = tweet['text'].encode('ascii','ignore').lower()
    timestamp   = int(tweet['timestamp'])
    words       = text.split(' ')
    buildGrams(words, timestamp)


def start():
    global updated
    global inserted    
    # Putting a lock file here to block hourly processing that has gone more than an hour

    try:
        lock = zc.lockfile.LockFile('/var/lock/ngramr')
        logger.info("started")

    except Exception,e:
        logger.warning("another ngramr running")
        sys.exit()

    #timedif     = datetime.datetime.now() - datetime.timedelta(hours=hours_ago)
    #lasthour    = timedif.strftime('%Y%m%d%H')
    #logger.info("working on hour %s" % lasthour)

    while True:
        for ordered_buckets in db.buckets.find({"unprocessed": {"$gt": 0}}).sort([('_id', 1)]):
            #n = 1
            count = db.hose.find({'processed': 0, 'bucket': ordered_buckets['_id']}).count()
            if (count > 0):
                #logger.info("grabbing %s tweets from %s" % (count, ordered_buckets['_id']))
                for tweet in db.hose.find({'processed': 0, 'bucket': ordered_buckets['_id']}, {"text": 1, "timestamp": 1}):
                    #if (n % 1000) == 0:
                    #    to_go = int(count - n)
                    #    logger.info("working on %s... %s to go" % (ordered_buckets['_id'], to_go))

                    text        = tweet['text'].encode('ascii','ignore').lower()
                    timestamp   = int(tweet['timestamp'])
                    words = text.split(' ')
            
                    buildGrams(words, timestamp)
            
                    db.hose.update({'_id': tweet['_id']},{'$set':{'processed': 1}})
                    #n = n +1
                total_grams = (updated + inserted) 
                logger.info("b:%s t:%s n:%s u:%s %s%% i:%s %s%%" % (ordered_buckets['_id'], count, total_grams, updated, round(float(updated) / float(total_grams) * 100), inserted, round( float(inserted) / float(total_grams) * 100)))
                updated = 0
                inserted = 0
            else:
                logger.info("caught up %s" % ordered_buckets['_id'])

            time.sleep(60)


def stop():
    
    logger.info("attempting to stop ngramr")
    
    try:
        f = open("/var/lock/ngramr", "r")
        for line in f:
            pid = line.strip()
            os.kill(int(pid), signal.SIGKILL)
            
        os.remove('/var/lock/ngramr')
        logger.info("ngramr stopped")
    
    except Exception, e:
        logger.error("Exception: %s" % str(e))


def buildGrams(words,timestamp):

    stop = stopwords.words('english')

    workinglist = [word for word in words if word not in stop and not word.isspace() and len(word)>3]
    if len(workinglist)>0:
        x = 1
        while (x <= 4):
            xgram = ngrams(workinglist, x)
            buildDistro(xgram, timestamp, x)
            x = x + 1


def buildDistro(xgram, timestamp, gram_length):
    global updated
    global inserted 
    distro = nltk.FreqDist(list(xgram))
    
    for k,v in sorted(set(distro.items()), key=operator.itemgetter(1)):
        gramString = ' '.join(k)
        query = {'gram': gramString}
        update = { 
            '$setOnInsert': {
                'firstSeen': int(timestamp),
                'gram_length': int(gram_length)
            }, 
            '$set': {
                'lastSeen': int(timestamp),
                'expireTime': datetime.datetime.utcnow()
            }, 
            '$max': { 'high': v }, 
            '$min': { 'low': v }, 
            '$addToSet': { 
                'chirps': {
                    'timestamp': int(timestamp), 
                    'count': v
                }
            }
        }
        r = db.grams.update(query, update, upsert=True)
        if r['updatedExisting']:
            updated = updated + 1
        else:
            inserted = inserted + 1

