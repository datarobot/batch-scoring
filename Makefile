.PHONY: test

flake8:
	flake8 datarobot_batch_scoring

.install-deps: requirements.txt
	pip install -U -r requirements.txt
	touch .install-deps

.install: .install-deps $(shell find datarobot_batch_scoring -type f)
	pip install -e .
	touch .install

test: .install
	py.test -v tests/

cov cover coverage: .install
	py.test -v --cov=datarobot_batch_scoring \
            --cov-report=term --cov-report=html tests/
	@echo "open file://`pwd`/htmlcov/index.html"
