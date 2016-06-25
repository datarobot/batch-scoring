1.8.3 (2016 July 6)
==================
* This release is compatible with Windows

* logs are now sent to two files within the directory where the script is run

1.8.2 (2016 June 16)
==================
* added --auto_sample option to find the n_samples automatically. 

1.8.1 (2016 June 15)
==================
* added --auto_sample option to find the n_samples automatically. 

* change how csv dialects are passed around in attempt to fix a bug on Windows.

1.8.0 (2016 June 13)
==================
* use chardet module `chardet <https://pypi.python.org/pypi/chardet>`_ to
  attempt to detect character encoding

* use standard lib csv module to attempt to discover CSV dialect

* use stream decoder and encoder in python 2 to transparently convert to utf-8

* provide a mode for sending all user messages to stdout

1.7.0 (2016 May)
==================
* separate process for disk IO and request payload serialization

* avoid codecs.getreader due to IO bottleneck

* dont parse CSV (fail fatally on multiline csv)

* multiline mode (to be renamed)

* keep_cols resolution


1.6.0 alpha (2016 April 29)
==================

* Get rid of gevent/asyncio, use thread-based networking

* Show path to logs on every unexpected error

* Convert cmdline argument parser from docopt to argparse

* Add configuration file support

* Refactor logging/ui

* Drop support of making predictions using 'v2' Modeling API

1.5.0
=====

* Fix bug under Python 2 where gevent was fatally failing on timeouts.

* Added timeout argument.

* Both asyncio and gevent now retry within the request exception handler.

* Authorization now checks schema too and thus we fail much earlier if
  input not correct.

1.4.0
=====

* Fix bug under Python 2 where gevent was silently dropping batches.

* Better checks if run completed successfully.

* Fail fast on missing column or dtype mismatch.

* Add naming of prediction column for regression.

* Fix ignore datarobot_key.

1.3.3
=====

* Update requirements for Python 3 to minimum versions.

1.3.2
=====

* Updated client side error reporting to show the status message when
  it returns formatted as JSON object instead of just the error code

1.3.1
=====

* Use utf8 encoding for CSV strings sent to prediction API server

1.3.0
=====

* Use CSV instead of JSON for better throughput and reduced memory
  footprint on the server-side.

1.2.1
=====

* Gevent dependency update to fix ssl bug on 2.7.9.

1.2.0
=====

* Setuptools support.

1.1.0
=====

* Use python logging and maintain a debug log to help support
  engineers trace errors.

1.0.2
=====

* More robust delimiter handling (whitelist).

* Dont segfault on non-splittable delimiter.

1.0.1
=====

* Set number of retries default to 3 instead of infinite.

* Fix: type -> task

1.0.0
=====

* Initial release
