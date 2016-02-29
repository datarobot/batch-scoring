.PHONY: test

flake8:
	flake8 datarobot_batch_scoring

test:
	pip install -r requirements.txt
	py.test -v --junit-xml=testResults.xml tests/
