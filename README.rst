datarobot_batch_scoring
=======================

A script to score CSV files via DataRobot's prediction API.

.. image:: https://coveralls.io/repos/github/datarobot/batch-scoring/badge.svg?branch=master
    :target: https://coveralls.io/github/datarobot/batch-scoring?branch=master

.. image:: https://travis-ci.org/datarobot/batch-scoring.svg?branch=master
    :target: https://travis-ci.org/datarobot/batch-scoring#master

.. image:: https://caniusepython3.com/project/datarobot_batch_scoring.svg

.. image:: https://badge.fury.io/py/datarobot_batch_scoring.svg
    :target: https://badge.fury.io/py/datarobot_batch_scoring.svg


How to install
--------------

Install or upgrade to last version: ::

    $ pip install -U datarobot_batch_scoring

How to install particular version: ::

    $ pip install datarobot_batch_scoring==x.y.z

Features
--------

* Concurrent requests (``--n_concurrent``)
* Pause/resume
* Gzip support
* Custom delimiters
* parallel processing


Running batch_scoring
---------------------

You can execute the ``batch_scoring`` or ``batch_scoring_sse`` command from the command line or
you can pass parameters to a ``batch_scoring`` or ``batch_scoring_sse`` script from .ini file. Place
the file in your home directory or the directory from which you are running the ``batch_scoring``
or ``batch_scoring_sse`` command. Use the syntax and arguments below to define the parameters.
Note that if you run the script and also execute via the command line, the command line parameters take priority.

The following table describes the syntax conventions; the syntax for running the script follows the table.
Please note that we supply to different scripts:

- ``batch_scoring`` is used to score over ORM and dedicated prediction instances.
- ``batch_scoring_sse`` is used to score over standalone prediction instances.  If you not familiar with the instance types please contact your support.

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

Additional recommended arguments
``[--verbose]  [--keep_cols=<keep_cols>]  [--n_concurrent=<n_concurrent>]``

Additional optional arguments
``[--out=<filepath>] [--api_version=<api_version>] [--pred_name=<string>] [--timeout=<timeout>] [—-create_api_token]  [--n_retry=<n_retry>] [--delimiter=<delimiter>]  [--resume]  [--skip_row_id]  [--output_delimiter=<delimiter>]``

Argument descriptions:
The following table describes each of the arguments:

============================== ========== ========= ===========
  Argument                     Standalone Dedicated Description
------------------------------ ---------- --------- -----------
 host=<host>                      \+         \+     Specifies the hostname of the prediction API endpoint (the location of the data from where to get predictions).
 user=<user>                      \-         \+     Specifies the username used to acquire the api-token. Use quotes if the name contains spaces.
 <import_id>                      \+         \-     Specifies imported model unique identification string. You can ask for ID your prediction administrator who did the import procedure.
 <project_id>                     \-         \+     Specifies the project identification string. You can find the ID: embedded in the URL that displays when you are in the Leaderboard (for example, ``https://<host>/projects/<project_id>/models``) or when the prediction API is enabled, from the example shown when you click **Deploy Model** for a specific model in the Leaderboard.
 <model_id>                       \-         \+     Specifies the model identification string. You can find the ID: embedded in the URL that displays when you are in the Leaderboard and have selected a model  (for example, ``https://<host>/projects/<project_id>/models/<model_id>``) or when the prediction API is enabled, from the example shown when you click Deploy Model for a specific model in the Leaderboard.
 <dataset_filepath>               \+         \+     Specifies the .csv input file that the script scores. It does this by submitting prediction requests against <host> using project <project_id> and model <model_id>.
 datarobot_key=<datarobot_key>    \-         \+     An additional datarobot_key for dedicated prediction instances. This argument is required when using on-demand workers on the Cloud platform, but not for Enterprise users.
 password=<pwd>                   \-         \+     Specifies the password used to acquire the api-token. Use quotes if the password  contains spaces. You must specify either the password or the api_token argument. To avoid entering your password each time you run the script, use the api_token argument instead.
 api_token=<api_token>            \-         \+     Specifies the api token for the requests; if you do not have a token, you must specify the password argument. You can retrieve your token from your profile on the **My Account** page.
 out=<filepath>                   \+         \+     Specifies the file name, and optionally path, to which the results are written. If not specified, the default file name is ``out.csv``, written to the directory containing the script. The value of the output file must be a single .csv file that can be gzipped (extension .gz).
 verbose                          \+         \+     Provides status updates while the script is running. It is recommended that you include this argument to track script execution progress. Silent mode (non-verbose) displays very little output.
 keep_cols=<keep_cols>            \+         \+     Specifies the column names to append to the predictions. Enter as a comma-separated list.
 n_samples=<n_samples>            \+         \+     DEPRECATED. Specifies the number of samples (rows) to use per batch. If not defined the "auto_sample" option will be used.
 n_concurrent=<n_concurrent>      \+         \+     Specifies the number of concurrent requests to submit. By default, 4 concurrent requests are submitted. Set ``<n_concurrent>`` to match the number of cores in the prediction API endpoint.
 create_api_token                 \+         \+     Requests a new API token. To use this option, you must specify the ``password`` argument for this request (not the ``api_token`` argument). Specifying this argument invalidates your existing API token and creates and stores a new token for future prediction requests.
 n_retry=<n_retry>                \+         \+     Specifies the number of times DataRobot will retry if a request fails. A value of -1, the default, specifies an infinite number of retries.
 pred_name=<pred_name>            \+         \+     Applies a name to the prediction column of the output file. If you do not supply the argument, the column name is blank. For binary predictions, only positive class columns are included in the output. The last class (in lexical order) is used as the name of the prediction column.
 skip_row_id                      \+         \+     Skip the row_id column in output.
 output_delimiter=<delimiter>     \+         \+     Specifies delimiter for output CSV. The special keyword "tab" can be used to indicate a tab delimited csv.
 timeout=<timeout>                \+         \+     The time, in seconds, that DataRobot tries to make a connection to satisfy a prediction request. When the timeout expires, the client (the batch_scoring command) closes the connection and retries, up to the number of times defined by the value of ``<n_retry>``. The default value is 30 seconds.
 delimiter=<delimiter>            \+         \+     Specifies the delimiter to recognize in the input .csv file (e.g., "--delimiter=,"). If not specified, the script tries to automatically determine the delimiter. The special keyword "tab" can be used to indicate a tab-delimited csv.
 resume                           \+         \+     Starts the prediction from the point at which it was halted. If the prediction stopped, for example due to error or network connection issue, you can run the same command with all the same arguments plus this ``resume`` argument. If you do not include this argument, and the script detects a previous script was interrupted mid-execution, DataRobot prompts whether to resume. When resuming a script, you cannot change the ``dataset_filepath``,  ``model_id``, ``project_id``, ``n_samples``, or ``keep_cols``.
 help                             \+         \+     Show usage help for the command.
 fast                             \+         \+     *Experimental*: Uses a faster csv processor. Note that this method does not support multiline csv.
 stdout                           \+         \+     Send all log messages to stdout.
 auto_sample                      \+         \+     Override the ``<n_samples>`` value and instead use chunks of roughly 1.5 MB to improve throughput. On by default.
 encoding                         \+         \+     Declare the dataset encoding. If an encoding is not provided, the batch_scoring script attempts to detect it (e.g., "utf-8", "latin-1", or "iso2022_jp"). `See the Python docs for a list of valid encodings <https://docs.python.org/3/library/codecs.html#standard-encodings>`_.
 skip_dialect                     \+         \+     Tell the batch_scoring script to skip csv dialect detection.
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
* The logs from each batch_scoring run are stored in the current working. All users will see a `datarobot_batch_scoring_main.log` log file. Windows users will see two additional log file, `datarobot_batch_scoring_batcher.log` and `datarobot_batch_scoring_writer.log`.


Supported Platforms
-------------------
The batch_scoring script is tested on Linux and Windows, but it should also work on OS X. Both Python 2.7 and Python 3.x are supported.


