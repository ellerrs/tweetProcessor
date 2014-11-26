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

timedif     = datetime.datetime.now() - datetime.timedelta(hours=25)
lasthour    = timedif.strftime('%Y%m%d%H')
filename = config.TWITTER_FILE_PATH + config.TWITTER_FILE_PREFIX + lasthour + '.txt.gz'


def log_info(msg) :
    timestamp = time.strftime('%Y%m%d:%H%M:%S')
    sys.stdout.write("%s: %s\n" % (timestamp,msg))


def dumpHourToDisk():
    
    records = db.a.find({'bucket':lasthour}).count();
    
    if records == 0:
        log_info("No records found. Not creating a file for %s" % lasthour)
    else:
        log_info("Found %s records. Creating file: %s" % (records, filename))
        output  = gzip.open( filename, 'wb')
        
        for x in db.a.find({'bucket':lasthour}):
            output.write(dumps(x))
            output.write('\n')
        output.close()
        log_info("Saved %s records to %s" % (records, filename))


def purgeDisk():

    if os.path.isfile(filename):
        log_info("Deleting %s" % filename)
        os.unlink(filename)
    else:
        log_info("%s already deleted" % filename)


def pushToS3(cleanDisk=True, cleanDB=True):

    if os.path.isfile(filename):
        conn = S3Connection(config.S3AUTH, config.S3KEY)
        bucket = conn.get_bucket(config.S3BUCKET)

        key = bucket.new_key(filename)
        key.set_contents_from_filename(filename)
        log_info("Pushing %s to %s" % (filename, config.S3BUCKET))

        if cleanDisk:
            purgeDisk()

        if cleanDB:
            purgeDB()

    else:
        log_info("Nothing to push. %s is missing" % filename)


def purgeDB():

    records = db.a.find({'bucket':lasthour}).count();
    log_info("Removing bucket: %s. Found %s records" % (lasthour, records))
    db.a.remove({'bucket': lasthour})


def main():
    me = singleton.SingleInstance()

    dumpHourToDisk()
    pushToS3()

    
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
