#!/bin/bash
#  On Linux this should be run inside Docker and triggered by ./test_pyinstaller_dockerized.sh
#  On OSX or other *nixes it would be run without docker
# 
set -e

REPO_BASE="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
cd $REPO_BASE
VERSION=$(grep -o -E '[^'\'']+' datarobot_batch_scoring/__init__.py | grep -v "version")
: "${VERSION:?Need to set VERSION non-empty}"
EXECUTABLE=datarobot_batch_scoring_"${VERSION}"_executables

rm -rf /tmp/test_pyinstaller 
mkdir -p /tmp/test_pyinstaller
cp dist/"${EXECUTABLE}"*.tar /tmp/test_pyinstaller

cd /tmp/test_pyinstaller/
tar -xf "${EXECUTABLE}"*.tar
cd "${EXECUTABLE}"*/
mkdir -p ~/bin
cp batch_scoring batch_scoring_sse batch_scoring_deployment_aware ~/bin
export PATH=$PATH:~/bin
cd /tmp/

# dry-run test that the executables can start
batch_scoring --help > /dev/null
batch_scoring_sse --help > /dev/null
batch_scoring_deployment_aware --help > /dev/null
batch_scoring  --host=https://foo.bar.com --user=fooy@example.com \
	--api_token=00000000000000000000000000000032 \
	--datarobot_key=000000000000000000000000000000000036 \
	000000000000000000000024 000000000000000000000024 \
	"${REPO_BASE}"/tests/fixtures/criteo_top30_1m.csv.gz \
	--dry_run --compress  -y

batch_scoring_sse  --host=https://foo.bar.com  000000000000000000000024 \
	"${REPO_BASE}"/tests/fixtures/criteo_top30_1m.csv.gz --dry_run --compress  -y

batch_scoring_deployment_aware  --host=https://foo.bar.com --user=fooy@example.com \
	--api_token=00000000000000000000000000000032 \
	--datarobot_key=000000000000000000000000000000000036 \
	000000000000000000000024 \
	"${REPO_BASE}"/tests/fixtures/criteo_top30_1m.csv.gz \
	--dry_run --compress  -y
