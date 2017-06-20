1.11.1 (unreleased)
===================

Enhancements
------------
* Logs now include version, retry attempts and whether output file was removed.
* New argument `no-resume` that allows you to start new batch-scoring run from scratch without being questioned about previous runs.



1.11.0 (2017 May 30)
=============

New Features
------------
* New parameter `field_size_limit` allows users to specify a larger maximum field
  size than the Python `csv` module normally allows. Users can use a larger number
  for this value if they encounter issues with very large text fields, for example.
  Please note that using larger values for this parameter may cause issues with
  memory consumption.

Bugfixes
--------
* Previously, files whose first few lines did not fit within 512KB would error during
  the auto-sampler (which finds a reasonable number of rows to send with each batch).
  This issue hsa been fixed by adding a fallback to a default of 10 lines per
  batch in these cases. This parameter can still be overridden by using the
  `n_samples` parameter.

* Fix issue when client error message wasn't logged properly.

1.10.2 (2017 May 9)
================
* Set default timeout on server response to infinity.

1.10.1 (2017 April 27)
================

* New semantic routes versioning support

* New prediction response schema support

* **Dropped support of DataRobot Prediction API < 3.0 version.**


1.10.0 (2017 January 27)
=================

* Independent prediction service support for scoring

1.9.1 (2017 January 6)
==================

* switched to supervisor + workers architecture, improving handling of errors and
  subprocess lifecycle control.

* Source code split into more mostly isolated modules.

1.9.0 (2016 December 1)
==================

* added 3rd parallel process which handles post-processing and writing of responses.
  This should greatly improve performance.

* add ability to compress data in transit

1.8.8 (2016 November 17)
==================
* --output_delimiter flag to set delimiter for output CSV. "tab" can be used
    for tab-delimited output

* --skip_row_id flag to skip row_id column in output

* fixed hang of batch-scoring script on CSV parse errors

* added summary of run at the end of script output with full list of errors,
    warnings and total stats.

* fixed error when trying to report multiline CSV error in fast mode

* Run all tests against Windows

1.8.7 (2016 November 1)
==================
* --pred_name parameter is documented. Potentially backward incompatible change:
    Previously, 1.0 class was used as positive result for binary predictions,
    now last class in lexical order is used

* Fixed memory leak and performance problem caused by unrestricted batch-generator

* internal check and error avoidance logic for requests that are too large

* docker and docker-compose files for dockerized run of tests and script

* auto sampler target batch size increased to 2.5M

1.8.6 (2016 August 23)
==================
* improve url parsing. You no longer need to include "/api" in the host argument.

* return more descriptive error messages when there is a problem

* include the version of the batch-scoring script in the user-agent header

1.8.5 (2016 July 28)
==================
* add option to define document encoding

* add option to skip csv dialect detection.

* make adjustment to sample size used by dialect and encoding detection

* use auto_sample as default unless "--n_samples" is defined

* allow "tab" command line arg keyword. e.g. "--delimiter=tab"

1.8.4 (2016 July 11)
==================
* minor performance improvement for nix users

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
