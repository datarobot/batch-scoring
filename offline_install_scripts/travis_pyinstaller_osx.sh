#!/bin/bash
set -e
set -x
# bundles up the OSX PyInstaller build
TRAVIS_COMMIT=$1
TRAVIS_TAG=$2
if [[ $TRAVIS_TAG ]]
then 
    VERS=$TRAVIS_TAG
else
    VERS=$TRAVIS_COMMIT
fi
REPO_BASE="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
cd $REPO_BASE
mkdir -p dist/datarobot_batch_scoring_executables
#  add documentation to zip
cp BATCH_SCORING_EXECUTABLE_README.txt dist/datarobot_batch_scoring_executables/
cd dist
cp batch_scoring batch_scoring_sse datarobot_batch_scoring_executables/
tar -cf datarobot_batch_scoring_"${VERS}"_executables.OSX.x86_64.tar datarobot_batch_scoring_executables
mkdir OSX
mv datarobot_batch_scoring_"${VERS}"_executables.OSX.x86_64.tar OSX/

