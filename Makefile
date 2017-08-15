CDIR := $(shell pwd)
export CDIR
.PHONY: test clean

.install-test-deps: requirements-test.txt
	pip install -U -r requirements-test.txt
	touch .install-test-deps

flake8: .install-test-deps
	flake8 datarobot_batch_scoring tests

.install: $(shell find datarobot_batch_scoring -type f)
	pip install .
	touch .install

test: .install .install-test-deps flake8
	# test that we can make the HTML for pypi. 1(info) might be too strict
	rst2html.py --report=1 --exit-status=1 README.rst > /dev/null
	py.test -v tests/

cov cover coverage: .install .install-test-deps flake8
	py.test -v --cov=datarobot_batch_scoring \
            --cov-report=term --cov-report=html tests/
	@echo "open file://`pwd`/htmlcov/index.html"

coverage-map: .install .install-test-deps
	coverage report -m

pyinstaller: clean
	#  On Linux use "make pyinstaller_dockerized"
	./offline_install_scripts/build_pyinstaller.sh

.install-docker-compose: clean
	pip install -r requirements-docker-compose.txt -q

pyinstaller_dockerized: .install-docker-compose
	docker-compose run --rm centos5pyinstaller \
	/batch-scoring/offline_install_scripts/build_pyinstaller_dockerized.sh 

offlinebundle:
	#  On Linux use "make offlinebundle_dockerized"
	./offline_install_scripts/build_offlinebundle.sh

offlinebundle_dockerized:
	docker run -i --rm -v ${CDIR}:/batch-scoring python:3.5 \
		/batch-scoring/offline_install_scripts/build_offlinebundle_dockerized.sh

build_release_dockerized: clean pyinstaller_dockerized offlinebundle_dockerized


test_offlinebundle_dockerized:
	docker run -i --net=none --rm -v ${CDIR}:/batch-scoring centos:7 /batch-scoring/offline_install_scripts/test_offlinebundle_dockerized.sh

test_pyinstaller_dockerized:
	docker run -i --net=none --rm -v ${CDIR}:/batch-scoring centos:7 /batch-scoring/offline_install_scripts/test_pyinstaller_dockerized.sh
	docker run -i --net=none --rm -v ${CDIR}:/batch-scoring centos:5 /batch-scoring/offline_install_scripts/test_pyinstaller_dockerized.sh
	docker run -i --net=none --rm -v ${CDIR}:/batch-scoring centos:7 /batch-scoring/offline_install_scripts/test_pyinstaller_dockerized.sh
	docker run -i --net=none --rm -v ${CDIR}:/batch-scoring ubuntu:12.04 /batch-scoring/offline_install_scripts/test_pyinstaller_dockerized.sh
	docker run -i --net=none --rm -v ${CDIR}:/batch-scoring ubuntu:16.04 /batch-scoring/offline_install_scripts/test_pyinstaller_dockerized.sh
	docker run -i --net=none --rm -v ${CDIR}:/batch-scoring pritunl/archlinux:latest /batch-scoring/offline_install_scripts/test_pyinstaller_dockerized.sh
	docker run -i --net=none --rm -v ${CDIR}:/batch-scoring gentoo/stage3-amd64 /batch-scoring/offline_install_scripts/test_pyinstaller_dockerized.sh 




clean:
	@rm -rf .install
	@rm -rf .install-test-deps
	@rm -rf .build_release_dockerized
	@rm -rf .install-docker-compose
	@rm -rf datarobot_batch_scoring.egg-info build/* dist/* 
	@rm -rf htmlcov
	@rm -rf .coverage
	@rm -f batch_scoring.spec batch_scoring_sse.spec
	@find . -name __pycache__ | xargs rm -rf
