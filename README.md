# Application of Python Twitter Tools


This is an ongoing project to build a downloader for Tweets into a relational Database.

It makes use of the pretty good implementation of [sixohsix's Python Twitter Tools](https://github.com/sixohsix/twitter)


## Authentication


Visit the Twitter developer page and create a new application: https://dev.twitter.com/apps/

There you will get your `CONSUMER_KEY` and `CONSUMER_SECRET`.

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
Consumer secret and Consumer keys are required for the OAuth authentication.
`write_rate` specifies the number of tweets to download before writing them to the database.
`max_caching_entries` refers to the number of hashtags IDs to keep in memory, it is needed to limit the number of queries to the database when we need to store for each tweet the IDs of the hashtags mentioned in it.     

	[Twitter_Config]
	dir = ./config
	twitter_creds = %(dir)s/my_app_credentials
	username = YourApplicationAccountUsername
	listener_username = YourUsername
	consumer_key = JvyS7DO2qd6NNTsXJ4E7zA
	consumer_secret = 9z6157pUbOBqtbm0A0q4r29Y2EYzIHlUwbF4Cl9c
	write_rate = 2000
	warn_rate = 100000
	max_caching_entries = 1000


## Python Twitter Tools as Submodule


[Sixohsix's Python Twitter Tools](https://github.com/sixohsix/twitter) is included as a submodule.
You can read more about [Submodules Git Tool](http://git-scm.com/book/en/Git-Tools-Submodules), basically when you receive the project, you get the directory that contains the submodule, but none of the files yet.

So you should enter the directory, and then init the submodule and update it as follows

    cd lib/twitter-python
    git submodule init
    git submodule update

Then the module need to be installe

    cd lib/twitter-python
    sudo python setup.py install


# Licence

This software as the Python Twitter Tools are released under an MIT License.
