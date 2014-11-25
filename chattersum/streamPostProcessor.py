from nltk.corpus import stopwords
import nltk
from boto.s3.connection import S3Connection
import gzip
from bson.json_util import dumps
import tweepy
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

timedif     = datetime.datetime.now() - datetime.timedelta(hours=0)
lasthour    = timedif.strftime('%Y%m%d%H')

def dumpHourToDisk():
    
    output  = gzip.open(config.TWITTER_FILE_PATH + config.TWITTER_FILE_PREFIX + lasthour + '.txt.gz', 'wb')

    for x in db.a.find({'bucket':lasthour}):
        output.write(dumps(x))
        output.write('\n')
    output.close()


def purgeDisk():
    os.unlink(config.TWITTER_FILE_PATH + config.TWITTER_FILE_PREFIX + lasthour + '.txt.gz')


def pushToS3(cleanDisk=False, cleanDB=False):
    conn = S3Connection(config.S3AUTH, config.S3KEY)
    bucket = conn.get_bucket(config.S3BUCKET)
    key = bucket.new_key(config.TWITTER_FILE_PATH + config.TWITTER_FILE_PREFIX + lasthour + '.txt.gz')
    ikey.set_contents_from_filename(config.TWITTER_FILE_PATH + config.TWITTER_FILE_PREFIX + lasthour + '.txt.gz')

    if cleanDisk:
        purgeDisk()

    if cleanDB:
        purgeDB()


def purgeDB():
    db.a.remove({'bucket': lasthour})


def main():
    me = singleton.SingleInstance()
    
#      for x in db.a.find({'bucket':lasthour}):
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
