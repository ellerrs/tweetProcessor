#!/bin/bash
cd /root/tweetProcessor
source ./env/bin/activate
./env/bin/python ./back/service.py -H
