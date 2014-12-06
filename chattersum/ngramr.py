

import config
import datetime
import getopt
import nltk
from nltk.corpus import stopwords
from nltk.util import ngrams
import operator
from pymongo import MongoClient
import sys
import time
import zc.lockfile


client = MongoClient( config.MONGO_HOST, config.MONGO_PORT)
client.twitter.authenticate( config.MONGO_USER, config.MONGO_PASS, mechanism='MONGODB-CR')
db = client.twitter

#words = nltk.word_tokenize(text)

def buildGramsStream(tweet):
    
    text        = tweet['text'].encode('ascii','ignore').lower()
    timestamp   = tweet['timestamp']
    words       = text.split(' ')
    buildGrams(words, timestamp)


def buildGramsBucket(hours_ago):
    
    timedif     = datetime.datetime.now() - datetime.timedelta(hours=hours_ago)
    lasthour    = timedif.strftime('%Y%m%d%H')
    
    for tweet in db.hose.find({'bucket': hours_ago}):
        text        = tweet['text'].encode('ascii','ignore').lower()
        timestamp   = tweet['timestamp']

    words = string.split(' ')
    buildGrams(words, timestamp)


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
	db.grams.update(query, update, upsert=True)

