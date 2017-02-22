#!/bin/bash
set -e
set -x
# bundles up the OSX PyInstaller build
TRAVIS_TAG=$1
REPO_BASE="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
cd $REPO_BASE
mkdir -p dist/datarobot_batch_scoring_executables
#  add documentation to zip
cp BATCH_SCORING_EXECUTABLE_README.txt dist/datarobot_batch_scoring_executables/
cd dist
cp batch_scoring batch_scoring_sse datarobot_batch_scoring_executables/
tar -cf datarobot_batch_scoring_"${TRAVIS_TAG}"_executables.OSX.x86_64.tar datarobot_batch_scoring_executables
