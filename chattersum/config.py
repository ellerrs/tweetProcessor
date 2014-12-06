# -*- coding: utf-8 -*-

import os

MONGO_HOST  = os.environ['MONGO_HOST']
MONGO_PORT  = int(os.environ['MONGO_PORT'])
MONGO_USER  = os.environ['MONGO_USER']
MONGO_PASS  = os.environ['MONGO_PASS']
MONGO_DB    = os.environ['MONGO_DB']

TWITTER_CONSUMER_KEY        = os.environ['TWITTER_CONSUMER_KEY']
TWITTER_CONSUMER_SECRET     = os.environ['TWITTER_CONSUMER_SECRET']
TWITTER_ACCESS_TOKEN        = os.environ['TWITTER_ACCESS_TOKEN']
TWITTER_ACCESS_TOKEN_SECRET = os.environ['TWITTER_ACCESS_TOKEN_SECRET']
TWITTER_FILE_PATH           = os.environ['TWITTER_FILE_PATH']
TWITTER_FILE_PREFIX         = os.environ['TWITTER_FILE_PREFIX']

S3AUTH      = os.environ['S3AUTH']
S3KEY       = os.environ['S3KEY']
S3BUCKET    = os.environ['S3BUCKET']
