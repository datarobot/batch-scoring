datarobot_batch_scoring
=======================

A script to score CSV files via DataRobot's prediction API.

.. image:: https://coveralls.io/repos/github/datarobot/batch-scoring/badge.svg?branch=master
    :target: https://coveralls.io/github/datarobot/batch-scoring?branch=master

.. image:: https://travis-ci.org/datarobot/batch-scoring.svg?branch=master
    :target: https://travis-ci.org/datarobot/batch-scoring#master

.. image:: https://caniusepython3.com/project/datarobot_batch_scoring.svg

.. image:: https://badge.fury.io/py/datarobot_batch_scoring.svg
    :target: https://pypi.python.org/pypi/datarobot_batch_scoring


Version Compatibility
---------------------
We aim to support as many versions of DataRobot as possible with each release of batch_scoring, but occasionally
there are changes in the backend that create incompatibilities. This chart is kept up-to-date with the version
compatibilities between this tool and versions of DataRobot. If you are unsure which version of DataRobot you
are using, please contact DataRobot support for assistance.

===================== =================
batch_scoring_version DataRobot Version
--------------------- -----------------
<=1.10                2.7, 2.8, 2.9
>=1.11, <1.13         3.0, 3.1+
>=1.13                2.7, 2.8, 2.9, 3.0, 3.1+
===================== =================

How to install
--------------

Install or upgrade to last version: ::

    $ pip install -U datarobot_batch_scoring

How to install particular version: ::

    $ pip install datarobot_batch_scoring==x.y.z

Alternative Installs
--------------------

We publish two alternative install methods on our releases_ page. These are for situations where internet is restricted or Python is unavailable.

:offlinebundle:
    For performing installations in environments where Python2.7 or Python3+ is available, but there is no access to the internet.
    Does not require administrative privileges or pip. Works on Linux, OSX or Windows.
    
    These files have "offlinebundle" in their name on the release page.

:PyInstaller:
    Using pyinstaller_ we build a single-file-executable that does not depend on Python. It only depends on libc and can be installed without administrative privileges.
    Right now we publish builds that work for most Linux distros made since Centos5. OSX and Windows are also supported.
    
    These files have "executables" in their name on the release page.

.. _releases: https://github.com/datarobot/batch-scoring/releases
.. _pyinstaller: http://www.pyinstaller.org/

Features
--------

* Concurrent requests (``--n_concurrent``)
* Pause/resume
* Gzip support
* Custom delimiters
* Parallel processing


Running the batch_scoring or batch_scoring_sse scripts
------------------------------------------------------

You can execute the ``batch_scoring`` or ``batch_scoring_sse`` command from the command line or
you can pass parameters to a ``batch_scoring`` or ``batch_scoring_sse`` script from the .ini file.
Place the .ini file in your home directory or the directory from which you are running the ``batch_scoring``
or ``batch_scoring_sse`` command. Use the syntax and arguments below to define the parameters.
Note that if you run the script and also execute via the command line, the command line parameters take priority.

The following table describes the syntax conventions; the syntax for running the script follows the table.
DataRobot supplies two scripts, each for a different application. Use:

- ``batch_scoring`` to score on dedicated prediction instances.
- ``batch_scoring_sse`` to score on standalone prediction instances. If you are unsure of your instance type, contact `DataRobot Support <https://support.datarobot.com/hc/en-us>`_.

============  =======
 Convention   Meaning
------------  -------
[ ]           Optional argument
< >           User supplied value
{ | }         Required, mutually exclusive
============  =======

Required arguments:

``batch_scoring --host=<host> --user=<user> <project_id> <model_id> <dataset_filepath> --datarobot_key=<datarobot_key> {--password=<pwd> | --api_token=<api_token>}``

``batch_scoring_sse --host=<host> <import_id> <dataset_filepath>``

Additional recommended arguments:

``[--verbose]  [--keep_cols=<keep_cols>]  [--n_concurrent=<n_concurrent>]``

Additional optional arguments:

``[--out=<filepath>] [--api_version=<api_version>] [--pred_name=<string>] [--timeout=<timeout>] [—-create_api_token]  [--n_retry=<n_retry>] [--delimiter=<delimiter>]  [--resume] [--no-resume] [--skip_row_id]  [--output_delimiter=<delimiter>]``

Argument descriptions:
The following table describes each of the arguments:

============================== ========== ========= ===========
  Argument                     Standalone Dedicated Description
------------------------------ ---------- --------- -----------
 host=<host>                      \+         \+     Specifies the hostname of the prediction API endpoint (the location of the data to use for predictions).
 user=<user>                      \-         \+     Specifies the username used to acquire the API token. Use quotes if the name contains spaces.
 <import_id>                      \+         \-     Specifies the unique ID for the imported model. If unknown, ask your prediction administrator (the person responsible for the import procedure).
 <project_id>                     \-         \+     Specifies the project identification string. You can find the ID embedded in the URL that displays when you are in the Leaderboard (for example, https://<host>/projects/<project_id>/models). Alternatively, when the prediction API is enabled, the project ID displays in the example shown when you click **Deploy Model** for a specific model in the Leaderboard.
 <model_id>                       \-         \+     Specifies the model identification string. You can find the ID embedded in the URL that displays when you are in the Leaderboard and have selected a model (for example, https://<host>/projects/<project_id>/models/<model_id>). Alternatively, when the prediction API is enabled, the model ID displays in the example shown when you click **Deploy Model** for a specific model in the Leaderboard.
 <dataset_filepath>               \+         \+     Specifies the .csv input file that the script scores. DataRobot scores models by submitting prediction requests against ``<host>`` using project ``<project_id>`` and model ``<model_id>``.
 datarobot_key=<datarobot_key>    \-         \+     An additional datarobot_key for dedicated prediction instances. This argument is required when using on-demand workers on the Cloud platform, but not for Enterprise users.
 password=<pwd>                   \-         \+     Specifies the password used to acquire the API token. Use quotes if the password contains spaces. You must specify either the password or the API token argument. To avoid entering your password each time you run the script, use the ``api_token`` argument instead.
 api_token=<api_token>            \-         \+     Specifies the API token for requests; if you do not have a token, you must specify the password argument. You can retrieve your token from your profile on the **My Account** page.
 api_version=<api_version>        \+         \+     Specifies the API version for requests. If omitted, defaults to current latest.
                                                    Override this if your DataRobot distribution doesn't support the latest API version.
                                                    Valid options are ``predApi/v1.0`` and ``api/v1``; ``predApi/v1.0`` is the default.
 out=<filepath>                   \+         \+     Specifies the file name, and optionally path, to which the results are written. If not specified, the default file name is ``out.csv``, written to the directory containing the script. The value of the output file must be a single .csv file that can be gzipped (extension .gz).
 verbose                          \+         \+     Provides status updates while the script is running. It is recommended that you include this argument to track script execution progress. Silent mode (non-verbose), the default, displays very little output.
 keep_cols=<keep_cols>            \+         \+     Specifies the column names to append to the predictions. Enter as a comma-separated list.
 n_samples=<n_samples>            \+         \+     Specifies the number of samples (rows) to use per batch. If not defined, the ``auto_sample`` option is used.
 n_concurrent=<n_concurrent>      \+         \+     Specifies the number of concurrent requests to submit. By default, the script submits four concurrent requests. Set ``<n_concurrent>`` to match the number of cores in the prediction API endpoint.
 create_api_token                 \+         \+     Requests a new API token. To use this option, you must specify the ``password`` argument for this request (not the ``api_token`` argument). Specifying this argument invalidates your existing API token and creates and stores a new token for future prediction requests.
 n_retry=<n_retry>                \+         \+     Specifies the number of times DataRobot will retry if a request fails. A value of -1, the default, specifies an infinite number of retries.
 pred_name=<pred_name>            \+         \+     Applies a name to the prediction column of the output file. If you do not supply the argument, the column name is blank. For binary predictions, only positive class columns are included in the output. The last class (in lexical order) is used as the name of the prediction column.
 skip_row_id                      \+         \+     Skip the row_id column in output.
 output_delimiter=<delimiter>     \+         \+     Specifies the delimiter for the output CSV file. The special keyword "tab" can be used to indicate a tab-delimited CSV.
 timeout=<timeout>                \+         \+     The time, in seconds, that DataRobot tries to make a connection to satisfy a prediction request. When the timeout expires, the client (the batch_scoring or batch_scoring_sse command) closes the connection and retries, up to the number of times defined by the value of ``<n_retry>``. The default value is 30 seconds.
 delimiter=<delimiter>            \+         \+     Specifies the delimiter to recognize in the input .csv file (e.g., "--delimiter=,"). If not specified, the script tries to automatically determine the delimiter. The special keyword "tab" can be used to indicate a tab-delimited CSV.
 resume                           \+         \+     Starts the prediction from the point at which it was halted. If the prediction stopped, for example due to error or network connection issue, you can run the same command with all the same arguments plus this ``resume`` argument. If you do not include this argument, and the script detects a previous script was interrupted mid-execution, DataRobot prompts whether to resume. When resuming a script, you cannot change the ``dataset_filepath``,  ``model_id``, ``project_id``, ``n_samples``, or ``keep_cols``.
 no-resume                        \+         \+     Starts the prediction from scratch disregarding previous run.
 help                             \+         \+     Shows usage help for the command.
 fast                             \+         \+     *Experimental*: Enables a faster .csv processor. Note that this method does not support multiline CSV files.
 stdout                           \+         \+     Sends all log messages to stdout. If not specified, the command sends log messages to the ``datarobot_batch_scoring_main.log`` file.
 auto_sample                      \+         \+     Override the ``<n_samples>`` value and instead uses chunks of roughly 2.5 MB to improve throughput. Enabled by default.
 encoding                         \+         \+     Specifies dataset encoding. If not provided, the batch_scoring or batch_scoring_sse script attempts to detect the decoding (e.g., "utf-8", "latin-1", or "iso2022_jp"). See the `Python standard encodings <https://docs.python.org/3/library/codecs.html#standard-encodings>`_ for a list of valid values.
 skip_dialect                     \+         \+     Specifies that the script skips CSV dialect detection and uses default "excel" dialect for CSV parsing. By default, the scripts do detect CSV dialect for proper batch generation on the client side.
 ca_bundle=<ca_bundle>            \+         \+     Specifies the path to a CA_BUNDLE file or directory with certificates of trusted Certificate Authorities (CAs) to be used for SSL verification.
                                                    Note: if passed a path to a directory, the directory must have been processed using the c_rehash utility supplied with OpenSSL.
 no_verify_ssl                    \+         \+     Disable SSL verification.
============================== ========== ========= ===========

Example::

    batch_scoring --host=https://mycorp.orm.datarobot.com/ --user="greg@mycorp.com" --out=pred.csv 5545eb20b4912911244d4835 5545eb71b4912911244d4847 /home/greg/Downloads/diabetes_test.csv
    batch_scoring_sse --host=https://mycorp.orm.datarobot.com/ --out=pred.csv 0ec5bcea7f0f45918fa88257bfe42c09 /home/greg/Downloads/diabetes_test.csv

Using the configuration file
----------------------------
The `batch_scoring` command checks for the existence of a batch_scoring.ini file at the location `$HOME/batch_scoring.ini` (your home directory) and the directory where you are running the script (working directory). If this file exists, the command uses the same arguments as those described above. If the file does not exist, the command proceeds normally with the command line arguments. The command line arguments have higher priority than the file arguments (that is, you can override file arguments using the command line).

The format of a `batch_scoring.ini` file is as follows::

  [batch_scoring]
  host=file_host
  project_id=file_project_id
  model_id=file_model_id
  user=file_username
  password=file_password


Usage Notes
-----------

* If the script detects that a previous script was interrupted in mid-execution, it will prompt whether to resume that execution.
* If no interrupted script was detected or if you indicate not to resume the previous execution, the script checks to see if the specified output file exists. If yes, the script prompts to confirm before overwriting this file.
* The logs from each ``batch_scoring`` and ``batch_scoring_sse`` run are stored in the current working directory. All users see a ``datarobot_batch_scoring_main.log`` log file. Windows users see two additional log files, ``datarobot_batch_scoring_batcher.log`` and ``datarobot_batch_scoring_writer.log``.
* Batch scoring won't work if there is only 1 feature in the scoring data. This issue is caused by limitations of standard python CSV parser. For resolving this issue, please add index column to the dataset - it'll be ignored in scoring, but will help it in parsing.


Supported Platforms
-------------------
datarobot_batch_scoring is tested on Linux and Windows and OS X. Both Python 2.7.x and Python 3.x are supported.

Recommended Python Version
--------------------------
Python 3.4 or greater is recommended, but all versions of Python 3 should work. Python 2.7.x. will work, but it sometimes errors decoding data
that Python 3 handles gracefully. Python 3 is also faster.

