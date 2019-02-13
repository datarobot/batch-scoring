#!/bin/bash
#  On Linux this should be run inside Docker and triggered by ./build_pyinstaller_dockerized.sh
#  On OSX or other *nixes it would be run without docker
set -e

REPO_BASE="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
cd $REPO_BASE
# venv as a module requires python3.4+
PYTHON=`which python3.5 || which python3`
rm -rf /tmp/TEMPVENV ./dist/pyinstaller

$PYTHON -m venv /tmp/TEMPVENV
. /tmp/TEMPVENV/bin/activate 

mkdir -p dist/pyinstaller
#  add documentation to zip
cp BATCH_SCORING_EXECUTABLE_README.txt dist/pyinstaller

pip install -U pip 
pip install -U urllib3[secure]
pip install -r requirements-base.txt -r requirements-pyinstaller.txt
pyinstaller -y --distpath=dist/pyinstaller --onefile -n batch_scoring batch_scoring.py
pyinstaller -y --distpath=dist/pyinstaller --onefile -n batch_scoring_sse batch_scoring_sse.py
pyinstaller -y --distpath=dist/pyinstaller --onefile -n batch_scoring_deployment_aware batch_scoring_deployment_aware.py

VERSION=$($PYTHON -c 'from datarobot_batch_scoring.__init__ import __version__ as v ; print(v)')
PLATFORM=$($PYTHON -c 'import platform; print(platform.system())')
ARCH=$($PYTHON -c 'import platform; print(platform.machine())')

cd dist/
NAME=datarobot_batch_scoring_"${VERSION}"_executables."${PLATFORM}"."${ARCH}"
mv pyinstaller "${NAME}"
zip -r -0 "${NAME}".zip "${NAME}"
tar -cf "${NAME}".tar "${NAME}"
