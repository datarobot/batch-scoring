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
	py.test -v tests/

cov cover coverage: .install .install-test-deps flake8
	py.test -v --cov=datarobot_batch_scoring \
            --cov-report=term --cov-report=html tests/
	@echo "open file://`pwd`/htmlcov/index.html"

clean:
	@rm -rf .install
	@rm -rf .install-test-deps
	@rm -rf datarobot_batch_scoring.egg-info
	@rm -rf htmlcov
	@rm -rf .coverage
	@find . -name __pycache__ | xargs rm -rf
