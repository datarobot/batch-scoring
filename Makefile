.PHONY: test

flake8:
	flake8 datarobot_batch_scoring tests

.install-deps: requirements.txt
	pip install -U -r requirements.txt
	touch .install-deps

.install: .install-deps $(shell find datarobot_batch_scoring -type f)
	pip install -e .
	touch .install

test: .install flake8
	py.test -v tests/

cov cover coverage: .install flake8
	py.test -v --cov=datarobot_batch_scoring \
            --cov-report=term --cov-report=html tests/
	@echo "open file://`pwd`/htmlcov/index.html"
