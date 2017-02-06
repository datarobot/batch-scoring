VERSION := $(shell python -c 'from datarobot_batch_scoring.__init__ import __version__ as v ; print(v)')
export VERSION
.PHONY: test

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
	rm -rf dist/pyinstaller dist/datarobot_batch_scoring_*_executables
	mkdir -p dist/pyinstaller
	cp OFFLINE_INSTALL_README.txt dist/pyinstaller
	( \
		PYTHON=`which python3.5 || which python3.4 || which python` ; \
		virtualenv --python="$${PYTHON}" TEMPVENV ; \
		. ./TEMPVENV/bin/activate; \
		pip install -U pip ; \
		pip install -r requirements.txt -r requirements-test.txt ; \
		pip install -U urllib3[secure] ; \
		pyinstaller -y --distpath=dist/pyinstaller --onefile -n batch_scoring batch_scoring.py ; \
		pyinstaller -y --distpath=dist/pyinstaller --onefile -n batch_scoring_sse batch_scoring_sse.py ; \
		cd dist/; \
		mv pyinstaller datarobot_batch_scoring_"$${VERSION}"_executables; \
		zip -r -0 datarobot_batch_scoring_"$${VERSION}"_executables.zip datarobot_batch_scoring_"$${VERSION}"_executables; \
	)

offlinebundle:
	@rm -rf ./TEMPVENV ./dist/offlinebundle
	@mkdir -p dist/offlinebundle/required_packages dist/offlinebundle/helper_packages
	@cp OFFLINE_INSTALL_README.txt dist/offlinebundle/
	wget https://bootstrap.pypa.io/get-pip.py
	@mv get-pip.py dist/offlinebundle/
	( \
		virtualenv TEMPVENV; \
		. ./TEMPVENV/bin/activate; \
		pip install -U pip setuptools; \
		python setup.py sdist; \
		pip download --dest=dist/offlinebundle/helper_packages --no-cache-dir  \
						pip setuptools virtualenv virtualenvwrapper wheel appdirs \
						pyparsing six packaging ; \
		pip download --dest=dist/offlinebundle/required_packages --no-cache-dir --no-binary :all: \
						dist/datarobot_batch_scoring-"$${VERSION}".tar.gz; \
		cd ./dist ; \
		zip -r -0 datarobot_batch_scoring_"$${VERSION}"_offlinebundle.zip offlinebundle ; \
	)

clean:
	@rm -rf .install
	@rm -rf .install-test-deps
	@rm -rf datarobot_batch_scoring.egg-info build/* dist/* ./TEMPVENV
	@rm -rf htmlcov
	@rm -rf .coverage
	@find . -name __pycache__ | xargs rm -rf
