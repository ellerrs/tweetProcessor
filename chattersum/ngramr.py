#!/usr/bin/env python
# -*- coding: utf-8 -*-

import config
import datetime
import getopt
import logging
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

    lock = zc.lockfile.LockFile('/var/lock/ngramr')
    logging.info("ngramr: started")

    #timedif     = datetime.datetime.now() - datetime.timedelta(hours=hours_ago)
    #lasthour    = timedif.strftime('%Y%m%d%H')
    #logging.info("ngramr: working on hour %s" % lasthour)

    while True:
        n = 0
        for tweet in db.hose.find({'processed': 0}):
            
            # throwing in a feedback loop so I can watch the log and see progress. 
            if (n % 5000==0):
                count = db.hose.find({'processed': 0}).count()
                if count == 0 :
                    logging.info("ngramr: no tweets to process")
                else:
                    logging.info("ngramr: %s queued for processing" % ( count))
            n = n + 1

            text        = tweet['text'].encode('ascii','ignore').lower()
            timestamp   = int(tweet['timestamp'])
            words = text.split(' ')
            
            buildGrams(words, timestamp)
            
            db.hose.update({'_id': tweet['_id']},{'$set':{'processed': 1}})
        
        logging.info("ngramr: caught up to the stream, sleeping for 60 seconds")
        logging.info("ngramr: updated:%s inserted:%s" % (updated, inserted))
        updated = 0
        inserted = 0
        time.sleep(60)


def stop():
    
    logging.info("streamr: attempting to stop ngramr")
    
    try:
        f = open("/var/lock/ngramr", "r")
        for line in f:
            pid = line.strip()
            os.kill(int(pid), signal.SIGKILL)
            
        os.remove('/var/lock/ngramr')
        logging.info("ngramr: ngramr stopped")
    
    except Exception, e:
        logging.error("ngramr: Exception: %s" % str(e))


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
                'lastSeen': int(timestamp)
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

