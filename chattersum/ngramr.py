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
    timestamp   = tweet['timestamp']
    words       = text.split(' ')
    buildGrams(words, timestamp)


def start():
    
    # Putting a lock file here to block hourly processing that has gone more than an hour

    lock = zc.lockfile.LockFile('/var/lock/ngramr')
    logging.info("ngramr: started")

    #timedif     = datetime.datetime.now() - datetime.timedelta(hours=hours_ago)
    #lasthour    = timedif.strftime('%Y%m%d%H')
    #logging.info("ngramr: working on hour %s" % lasthour)

    max_to_process = 10000
    while True:
        count = db.hose.find({'processed': False}).count()
        if count == 0 :
            logging.info("ngramr: no tweets to process")
        else:
            logging.info("ngramr: found %s tweets to process. Working on the first %s" % (count, max_to_process))

        n = 1

        for tweet in db.hose.find({'processed': False}).limit(max_to_process):

            # throwing in a feedback loop so I can watch the log and see progress. 
            if (n % 100==0):
                logging.debug("ngramr: processed %s. only %s to go" % (n, (count - n)))
            n = n + 1

            text        = tweet['text'].encode('ascii','ignore').lower()
            timestamp   = tweet['timestamp']
            words = text.split(' ')
            
            buildGrams(words, timestamp)
            
            db.hose.update({'_id': tweet['_id']},{'$set':{'processed':True}})
        
        logging.info("ngramr: sleeping for 60 seconds")
        logging.info("ngramr: u:%s i:%s" % (updated, inserted))
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
    
    distro = nltk.FreqDist(list(xgram))
    
    for k,v in sorted(set(distro.items()), key=operator.itemgetter(1)):
        gramString = ' '.join(k)
        query = {'gram': gramString}
        update = { 
            '$setOnInsert': {
                'firstSeen': timestamp,
                'gram_length': gram_length
            }, 
            '$set': {
                'lastSeen': timestamp
            }, 
            '$max': { 'high': v }, 
            '$min': { 'low': v }, 
            '$addToSet': { 
                'chirps': {
                    'timestamp': timestamp, 
                    'count': v
                }
            }
        }
        r = db.grams.update(query, update, upsert=True)
        if r['updatedExisting']:
            global updated 
            updated = updated + 1
        else:
            global inserted
            inserted = inserted + 1

