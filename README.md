# Application of Python Twitter Tools
This is an ongoing project to build a downloader for Tweets into a relational Database.

It makes use of the pretty good implementation of [sixohsix's Python Twitter Tools](https://github.com/sixohsix/twitter)

It is designed to download tweets from [the streaming API](https://dev.twitter.com/docs/streaming-apis), save english tweets in a MySQL database, and to notify you now and then about the status of the download.


## Set Up
To have this running, you should :
   0. Download the code – including [sixohsix's Python Twitter Tools](https://github.com/sixohsix/twitter ) and make sure to have all libraries up and running;
   1. Set up a Twitter APP and obtain an API KEY;
   2. Set up the MySQL Database, the schema is in the `db` folder;
   3. Change the configuration file following your setup;
   4. Run `stream_tweet.py`

Some notes about step *1* and *3* are below.


## Authentication
Visit the Twitter developer page and create a new application: https://dev.twitter.com/apps/

There you will get your `CONSUMER_KEY` and `CONSUMER_SECRET`.
Make sure your app and token have both read and write permissions.

Then edit the file

    config/twitter_config.cfg

Currently the project contains a little script to materialise Twitter OAuth credentials, it is:

    oauth_dance.py

Run it first once you have updated the config file with your consumer key and secret.
A few HTTP calls to twitter are required to run this.
Please see the twitter.oauth_dance module to see how this is done.
If you are making a command-line app, you can use the oauth_dance() function directly.

Performing the "oauth dance" gets you an oauth token and oauth secret that authenticate the user with Twitter.
This process should save these on a file for later, so that the user doesn't have to do the oauth dance again.

Keep them secret!

## Authentication pt. 2
Username and Password authentication mode has been deprecated and removed

## Configuration
The configuration file is organized as follows:

The first section contans the authentication parameters to the database, where tweets and data relative to them are stored.

    [DB_Config]
    db_host = localhost
    db_name = twitter
    db_user = username
    db_password = th3p4$$w0rd


The second section instead contains the path to the configuration directory, relative to the root of the project.
The name of the file where the OAuth credentials are stored.
The username of the twitter account under which the application is running.
The Twitter username to which the application will send a Direct Message once the number of tweets declared into `warn_rate` have been downloaded.
You want this to notify you that the application is still running and to stay updated about its actual performances.
There is flag for that: `direct_message_notification`.
The `language` field filters tweets by their language code - use `xx` as value to download every tweet.

Consumer secret and Consumer keys are required for the OAuth authentication.
`write_rate` specifies the number of tweets to download before writing them to the database.
`max_caching_entries` refers to the number of hashtags IDs to keep in memory, it is needed to limit the number of queries to the database when we need to store for each tweet the IDs of the hashtags mentioned in it.
When activating the `demo_mode` instead of saving anything to the database, you will just received a print out on screen of the tweets that have been downloaded.

    [Twitter_Config]
    dir = ./config
    twitter_creds = %(dir)s/my_app_credentials
    username = YourApplicationAccountUsername
    listener_username = YourUsername
    consumer_key = JvyS7DO2qd6NNTsXJ4E7zA
    consumer_secret = 9z6157pUbOBqtbm0A0q4r29Y2EYzIHlUwbF4Cl9c
    direct_message_notification = True
    language = en
    demo_mode = On
    write_rate = 2000
    warn_rate = 100000
    max_caching_entries = 1000

## Issues
### 401 Unauthorized
If you encounter  problems with OAuth and a wild

    HTTP Error 401: Unauthorized

appears, the answer (in 90% of the cases) is:

    sudo  ntpdate -b pool.ntp.org

Yes, your system has lost a minute too much!


## Python Twitter Tools as Submodule
[Sixohsix's Python Twitter Tools](https://github.com/sixohsix/twitter) is included as a submodule.
You can read more about [Submodules Git Tool](http://git-scm.com/book/en/Git-Tools-Submodules), basically when you receive the project, you get the directory that contains the submodule, but none of the files yet.
You can even install the library independently - this submodule could be removed in the future.
For now you first init the submodule from the root directory of the project and update it as follows

    git submodule init
    git submodule update

Then the module need to be installed

    cd lib/twitter-python
    sudo python setup.py install


## Other dependencies
To install other dependencies

    `sudo apt-get install python-mysqldb` to install MySQL-python
    `sudo apt-get install python-dateutil` to install Dateutil

# Licence
This software as the Python Twitter Tools are released under an MIT License.


