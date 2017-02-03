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
	pip uninstall -y datarobot_batch_scoring || true
	pip install -r requirements.txt -r requirements-test.txt
	pyinstaller -y --onefile -n batch_scoring batch_scoring.py
	pyinstaller -y --onefile -n batch_scoring_sse batch_scoring_sse.py

clean:
	@rm -rf .install
	@rm -rf .install-test-deps
	@rm -rf datarobot_batch_scoring.egg-info build/* dist/*
	@rm -rf htmlcov
	@rm -rf .coverage
	@find . -name __pycache__ | xargs rm -rf
