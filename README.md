# Application of Python Twitter Tools


This is an ongoing project to build a downloader for Tweets into a relational Database.

It makes use of the pretty good implementation of [sixohsix's Python Twitter Tools](https://github.com/sixohsix/twitter)


## Authentication


Visit the Twitter developer page and create a new application: https://dev.twitter.com/apps/

There you will get you a CONSUMER_KEY and CONSUMER_SECRET.

Then edit the file

    config/twitter_config.cfg

Currently it contains a little script to materialise Twitter OAuth credentials, it is:

    oauth_dance.py


A few HTTP calls to twitter are required to do this.
Please see the twitter.oauth_dance module to see how this is done.
If you are making a command-line app, you can use the oauth_dance() function directly.

Performing the "oauth dance" gets you an ouath token and oauth secret that authenticate the user with Twitter.
This process should save these for later so that the user doesn't have to do the oauth dance again.

Keep them secret!


## Python Twitter Tools as Submodule


[Sixohsix's Python Twitter Tools](https://github.com/sixohsix/twitter) are included as a submodule.
You can read more about [Submodules Git Tool](http://git-scm.com/book/en/Git-Tools-Submodules), basically when you receive the project, you get the directories that contain the submodule, but none of the files yet:

So you should enter the directory, and then init the sbmodule and update it.

    cd lib/twitter-python
    git submodule init
    git submodule update




# Licence

This software as the Python Twitter Tools are released under an MIT License.
