#!/bin/bash
#  On Linux this should be run inside Docker and triggered by ./build_pyinstaller_dockerized.sh
#  On OSX or other *nixes it would be run without docker
set -e
REPO_BASE="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
cd $REPO_BASE
VERSION=$(grep -o -E '[^'\'']+' datarobot_batch_scoring/__init__.py | grep -v "version")
: "${VERSION:?Need to set VERSION non-empty}"
BUNDLE=datarobot_batch_scoring_"${VERSION}"_offlinebundle

rm -rf /tmp/test_offlinebundle 
mkdir -p /tmp/test_offlinebundle/
cp dist/"${BUNDLE}".tar /tmp/test_offlinebundle/

cd /tmp/test_offlinebundle/
tar -xf "${BUNDLE}".tar
cd batch_scoring_offlinebundle
which python || whereis python
python --version
python get-pip.py --user --no-index --find-links=helper_packages/
export PATH=~/.local:$PATH
python -m pip install --user --no-index --find-links=helper_packages/ helper_packages/*
# Now the user will always be able to use the commands:
pip install --user --no-index --find-links=required_packages/ required_packages/* 
cd ~/
pip --help > /dev/null
virtualenv --help > /dev/null
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
