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

timedif     = datetime.datetime.now() - datetime.timedelta(hours=25)
lasthour    = timedif.strftime('%Y%m%d%H')
filename = config.TWITTER_FILE_PATH + config.TWITTER_FILE_PREFIX + lasthour + '.txt.gz'


def log_info(msg) :
    timestamp = time.strftime('%Y%m%d:%H%M:%S')
    sys.stdout.write("%s: %s\n" % (timestamp,msg))


def dumpHourToDisk():
 
    tweetString = []
   
    for x in db.hose.find():
        tweet = x['text'].encode('ascii','ignore').lower()
	tweetString.append(tweet)
#	words = nltk.word_tokenize(tweet)

#	stops = stopwords.words('english')

#	workinglist = [word for word in words if word.isalpha() and word not in stops]

#       tokens = [token.lower() for token in newlist if len(token)>2]
#	bigram = nltk.bigrams(words)

#	trigram = nltk.trigrams(words)
#	print (dict) trigram
#	fdist = nltk.FreqDist(list(trigram))
#	for k,v in fdist.items():
#	    print k,v 
#       print list(set(tri_tokens))
#       print [(item, tri_tokens) for item in sorted(set(tri_tokens))]
#       print list(grams)
    string = ' '.join(tweetString)

#    words = nltk.word_tokenize(string)
    words = string.split(' ')

#    stops = stopwords.words('english')

#    workinglist = [word for word in words if word.isalpha() and word not in stops]

#       tokens = [token.lower() for token in newlist if len(token)>2]
    bigram = nltk.bigrams(words)
    trigram = nltk.trigrams(words)
    quadgram = ngrams(words, 4)

#       print (dict) trigram

    fdist = nltk.FreqDist(list(quadgram))
    for k,v in sorted(set(fdist.items()), key=operator.itemgetter(1)):
        print k,v

        


def main():
    me = singleton.SingleInstance()
    dumpHourToDisk()

    
#      for x in db.hose.find({'bucket':lasthour}):
#       words = nltk.word_tokenize(x['text'].encode('ascii', 'ignore'))
#       stop = stopwords.words('english')
#       newlist = [w for w in words if w.isalpha() and w not in stop]
#       tokens = [token.lower() for token in newlist if len(token)>2]
#       bi_tokens = nltk.bigrams(tokens)
#       tri_tokens = nltk.trigrams(tokens)
#       print list(tri_tokens)
#       print list(set(tri_tokens))
#       print [(item, tri_tokens) for item in sorted(set(tri_tokens))]
#       print list(grams)


if __name__ == '__main__':
    try:
      main()
    except Exception,e:
      print str(e)
