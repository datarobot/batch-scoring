#!/bin/bash
# Usage: invoked my "make offlinebundle_dockerized"
# it is meant to run inside a docker container where it sets up the environment
# it then calls the "make offlinebundle" build command
apt-get update
apt-get install -y zip unzip -q
HUID=`ls -nd /batch-scoring | cut --delimiter=' ' -f 3`
useradd -m -s /bin/bash -u $HUID user 
su user -c -l "cd /batch-scoring; make offlinebundle"
exit