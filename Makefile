CDIR := $(shell pwd)
export CDIR
.PHONY: test clean

.install-test-deps: requirements-test.txt
	pip install -U -r requirements-test.txt
	touch .install-test-deps

flake8: .install-test-deps
	flake8 datarobot_batch_scoring tests

.install: $(shell find datarobot_batch_scoring -type f)
	pip install -e .
	touch .install

test: .install .install-test-deps flake8
	# test that we can make the HTML for pypi. 1(info) might be too strict
	rst2html.py --report=1 --exit-status=1 README.rst > /dev/null
	py.test -v tests/

cov cover coverage: .install .install-test-deps flake8
	py.test -v --cov=datarobot_batch_scoring \
            --cov-report=term --cov-report=html tests/
	@echo "open file://`pwd`/htmlcov/index.html"

pyinstaller: clean
	#  On Linux use "make pyinstaller_dockerized"
	./offline_install_scripts/build_pyinstaller.sh

pyinstaller_dockerized: clean
	docker run --rm -v ${CDIR}:/batch-scoring pyinstaller-centos5-py35-build \
		/batch-scoring/offline_install_scripts/build_pyinstaller_dockerized.sh

offlinebundle:
	#  On Linux use "make offlinebundle_dockerized"
	./offline_install_scripts/build_offlinebundle.sh

offlinebundle_dockerized:
	docker run --rm -v ${CDIR}:/batch-scoring python:3.5 \
		/batch-scoring/offline_install_scripts/build_offlinebundle_dockerized.sh

build_release_dockerized: pyinstaller_dockerized offlinebundle_dockerized

clean:
	@rm -rf .install
	@rm -rf .install-test-deps
	@rm -rf datarobot_batch_scoring.egg-info build/* dist/* 
	@rm -rf htmlcov
	@rm -rf .coverage
	@rm -f batch_scoring.spec batch_scoring_sse.spec
	@find . -name __pycache__ | xargs rm -rf
