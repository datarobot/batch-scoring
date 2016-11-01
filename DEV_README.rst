Installation
------------

From source
^^^^^^^^^^^

Create virtualenv::

    $ mkvirtualenv batch_scoring

Install package in virtualenv::

    $ pip install -e .

Now ``batch_scoring`` script should be available in your PATH.

You can also create virtualenvs with different python versions::

    $ mkvirtualenv batch_scoring_3 -p /usr/bin/python3.5

Using Docker containers
~~~~~~~~~~~~~~~~~~~~~~~

Build containers::

    $ docker-compose build

Run tests in 2.7 and 3.5::

    $ docker-compose run python27 make test
    $ docker-compose run python35 make test

Run batch-scoring from container::

    $ docker-compose run python27 batch-scoring {args..}
    $ docker-compose run python35 batch-scoring {args..}

Deployment
----------

Cut a release candidate

  - update ``__version__`` in ``datarobot_batch_scoring/__init__.py``
  - perform acceptance tests
  - tag release
  - push a tag to GitHub

Travis bot runs authomated tests and publish new version on PyPI when
tests are passed.

Packaging for windows
---------------------
Use Python 3.4 only -- py2exe doesn't work with Python 3.5:

1. install py2exe (pip install py2exe)
2. install requirements (pip install -r requirements34.txt)
3. build dist (python setup.py py2exe)
4. Distribute content of *dist* folder
5. On target machine download "Microsoft Visual C++ 2010 Redistributable Package (x64)" (https://www.microsoft.com/en-us/download/details.aspx?id=14632) and install
6. Enjoy!
