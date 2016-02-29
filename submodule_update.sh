#!/bin/bash
cd lib/twitter-python/
pwd
git checkout master
git pull
cd ../../
pwd
git commit -am "Pulled down update to lib/twitter-python/"
