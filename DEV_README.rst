Installation
------------

From source
^^^^^^^^^^^

Create virtualenv::

    $ mkvirtualenv batch_scoring

Install package in virtualenv::

    $ pip install -e .

Now ``batch_scoring`` script should be available in your PATH.

From sdist
^^^^^^^^^^

For Python 2.7::

    $ python setup.py sdist

For Python 3::

    $ python3.X setup.py sdist

Install via pip::

    $ pip install dist/datarobot_batch_scoring-1.X.X.tar.gz

Run from source
^^^^^^^^^^^^^^^

If you want to run the script without installation you have to
specify the ``batch_scoring`` module in the python interpreter::

    $ python -m datarobot_scoring.batch_scoring --help


Install From S3
^^^^^^^^^^^^^^^

When trying to determine the path, first check the datarobot Amazon S3
bucket to determine the most current version.  The path to S3 is:
https://s3.amazonaws.com/datarobot_public/packages/

To get access to S3 you must be authorized and have login credentials
to obtain this you must reach out to Tom or Ulises.

For Python 2.7::

    $ pip install -U https://s3.amazonaws.com/datarobot_public/packages/datarobot_batch_scoring-X.X.X-py2.tar.gz

For Python 3::

    $ pip install -U https://s3.amazonaws.com/datarobot_public/packages/datarobot_batch_scoring-X.X.X-py3.tar.gz

Deployment
----------

1. Cut a release candidate

  - update setup.py & datarobot_scoring/batch_scoring.py
  - acceptance testing
  - tag release (& push tag)

2. Create sdist tarball

  - Make py3 tarball::

    $ python3.3 setup.py sdist
    $ mv dist/datarobot_scoring-X.X.X.tar.gz dist/datarobot_scoring-X.X.X-py3.tar.gz

  - Make py2 tarball::

    $ python2.7 setup.py sdist
    $ mv dist/datarobot_scoring-X.X.X.tar.gz dist/datarobot_scoring-X.X.X-py2.tar.gz

3. Distribute tarballs

  - TODO
