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

        # install word filters used by nltk
        $ python -m nltk.downloader all
        
        # new version of pymongo 2.8
        $ pip install git+git://github.com/mongodb/mongo-python-driver.git@2.8rc0

        
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

* Mongodb --stuff--
    .. code-block:: bash
    
        db.createCollection('hose');
        db.createCollection('grams');
        db.createCollection('buckets');

        db.grams.ensureIndex({"gram": 1});
        db.grams.ensureIndex({"lastSeen": 1, "high": 1});
        db.grams.ensureIndex({"expireTime": 1}, { expireAfterSeconds: 300});

        db.hose.ensureIndex({"processed": 1});
        db.hose.ensureIndex({"bucket": 1});

        // create user for app
        db.createUser({ user: "<user>", pwd: "<password>", roles: [{ role: "readWrite",                                               db: "<database>"}]})

        // create root mongo user
        db.createUser({ user: "<user>", pwd: "<password>", roles: [{ role: "root", db: "                                              admin" }]})
