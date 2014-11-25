TweetProcessor
--------------

A small python project collect and process the twitter stream.

* GitHub: https://github.com/Chattersum/tweetProcessor
* Free software: BSD license

Features
--------
Yes, there are some. Very likely there will be more. Or not. Its hard to tell. 

* Setup the project
    .. code-block:: bash
        
        # create a directory for the project
        $ mkdir ./<myproject>
        $ cd ./<myproject>
        
        # clone the repo
        $ git clone https://github.com/Chattersum/tweetProcessor .
        
        # create a virtual env and activate
        $ virtualenv env
        $ . ./env/bin/activate
        
        # install the required packages from setup.py
        $ python ./setup.py install
        
* Local ENV configuration
    .. code-block:: bash
        # edit the ./env/bin/activate file
        $ vim ./env/bin/activate
        
        # add/edit the following to suit, replacing <>
        export MONGO_HOST=localhost
        export MONGO_PORT=27017
        export MONGO_USER=
        export MONGO_PASS=
        export MONGO_DB=
        
        export TWITTER_CONSUMER_KEY=
        export TWITTER_CONSUMER_SECRET=
        export TWITTER_ACCESS_TOKEN=
        export TWITTER_ACCESS_TOKEN_SECRET=
        export TWITTER_FILE_PATH=
        export TWITTER_FILE_PREFIX=
        
        export S3AUTH=
        export S3KEY=
        export S3BUCKET=
