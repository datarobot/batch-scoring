DataRobot batch_scoring
=======================

A script to score CSV files via DataRobot's prediction API.

Installation
------------

From PyPI
^^^^^^^^^

::

    $ pip install -U datarobot_batch_scoring

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

Features
--------

  * Concurrent requests (``--n_concurrent``)
  * Pause/resume
  * Gzip support
  * Custom delimiters

Usage
-----

::
    Usage: batch_scoring [--host=<host>] [--user=<user>]
                         [--password=<pwd>] [--api_token=<api_token>]
                         [--datarobot_key=<datarobot_key>] [--verbose]
                         [--n_samples=<n_samples>] [--n_retry=<n_retry>]
                         [--n_concurrent=<n_concurrent>]
                         [--out=<out>]
                         [--api_version=<api_version>]
                         [--create_api_token] [--keep_cols=<keep_cols>]
                         [--delimiter=<delimiter>]
                         {project_id}
                         {model_id}
                         {dataset}
                         [--resume|--cancel]

Batch score ``dataset`` by submitting prediction requests against ``host``
using model ``model_id``. It will send batches of size ``n_samples``.

Set ``n_samples`` such that the round-trip is roughly 10sec (see
verbose output).

Set ``n_concurrent`` to match the number of cores in the prediction
API endpoint.

The dataset has to be a single CSV file that can be gzipped (extension
'.gz').

The output ``out`` will be a single CSV files but remember that
records might be unordered.

Arguments::

  --host=<host>    The host to test [default: https://beta.datarobot.com/api].
  --api_version=<api_version>    The API version [default: v1]
  --datarobot_key=<datarobot_key>   An additional datarobot_key for dedicated prediction instances.
  --user=<user>  The username to acquire the api-token; if none prompt.
  --password=<pwd>  The password to acquire the api-token; if none prompt.
  --n_samples=<n_samples>  The number of samples per batch [default: 1000].
  --n_retry=<n_retry>  The number of retries if a request failed; -1 means infinite [default: 3].
  --n_concurrent=<n_concurrent>  The number of concurrent requests to submit [default: 4].
  --api_token=<api_token>  The api token for the requests; if none use <pwd> to get token.
  --out=<out>  The file to which the results should be written [default: out.csv].
  --keep_cols=<keep_cols>  A comma separated list of column names to append to the predictions.
  --delimiter=<delimiter>  Delimiter to use. If empty, will try to automatically determine this [default: ,].
  {project_id}  the project ID number
  {model_id} the model ID number
  {dataset} the filename of the records you want to fetch predictions for

Options::

  -h --help
  -v --verbose  Verbose output
  -c --create_api_token  If set we will request a new api token.
  -r --resume   Resume a checkpointed run.
  -c --cancel   Cancel a checkpointed run.

Example::

  batch_scoring --host=https://beta.datarobot.com/api --user="greg@datarobot.com" --out=pred.csv 5545eb20b4912911244d4835 5545eb71b4912911244d4847 ~/Downloads/diabetes_test.csv
