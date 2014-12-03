from nltk.corpus import stopwords
import nltk
from nltk.util import ngrams
from boto.s3.connection import S3Connection
import gzip
from bson.json_util import dumps
import tweepy
import operator
import time
import datetime
import sys
import os
import json
import config
from tendo import singleton
from pymongo import MongoClient

client = MongoClient( config.MONGO_HOST, config.MONGO_PORT)
client.twitter.authenticate( config.MONGO_USER, config.MONGO_PASS, mechanism='MONGODB-CR')
db = client.twitter

def buildGramsStream(tweet):
    text = tweet['text'].encode('ascii','ignore').lower()
    timestamp = tweet['timestamp']
    words = text.split(' ')
    #words = nltk.word_tokenize(text)
    buildGrams(words, timestamp)


def buildGramsBucket(bucket):
    tweetString = []

    for x in db.hose.find():
        tweet = x['text'].encode('ascii','ignore').lower()
        tweetString.append(tweet)

    string = ' '.join(tweetString)
    #words = string.split(' ')
    words = nltk.work_tokenize(string)


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
