Installation
------------

From source
^^^^^^^^^^^

Create virtualenv::

    $ mkvirtualenv batch_scoring

Install package in virtualenv::

    $ pip install -e .

Now ``batch_scoring`` script should be available in your PATH.

Install the package itself::

    $ python setup.py install

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

Release 
-------

1. update ``__version__`` in ``datarobot_batch_scoring/__init__.py``
2. perform acceptance tests, wait for tests to go green, get sign-off
3. merge PR in github and get SHA
4. tag release and push a tag to GitHub

  - ``git tag vX.Y.Z  <SHA>``
  - ``git push --tags``
  - This triggers Travis and Appveyor bots to runs automated tests and publish new version on PyPI when tests are passed.
  - Travis and Appveyor also build the PyInstaller executables for Windows and OSX and push to S3.

5. build the **PyInstaller** and **Offlinebundle** for Linux

  1. build the image for PyInstaller with ``docker-compose build centos5pyinstaller``
  2. build both releases with ``make build_release_dockerized`` (does not work on MacOS, use Linux instead). This will add the following to the dist dir:

    - datarobot_batch_scoring_<TAG>_offlinebundle.zip
    - datarobot_batch_scoring_<TAG>_offlinebundle.tar
    - datarobot_batch_scoring_<TAG>_executables.Linux.x86_64.tar
    - datarobot_batch_scoring_<TAG>_executables.Linux.x86_64.zip

6. Collect the **PyInstaller** artifacts for OSX and Windows from S3

    When Travis and Appveyor finish their builds they upload the pyinstaller artifacts to a S3 bucket called ``datarobot-batch-scoring-artifacts``.
    The OSX builds are in the ``osx-tagged-artifacts`` dir. The Windows artifacts are in ``windows-tagged-artifacts``. Both should have the tag name in their name.
    Collect the artifacts:

    - datarobot_batch_scoring_<TAG>_executables.Windows.x86_64.zip
    - datarobot_batch_scoring_<TAG>_executables.OSX.x86_64.tar

7. upload the builds produced by step 5 and 6

  1. find new version tag that was pushed at https://github.com/datarobot/batch-scoring/tags
  2. ``create`` the release on github for selected tag and attach 6 files:

    - datarobot_batch_scoring_<TAG>_executables.Linux.x86_64.tar
    - datarobot_batch_scoring_<TAG>_executables.Linux.x86_64.zip
    - datarobot_batch_scoring_<TAG>_offlinebundle.zip
    - datarobot_batch_scoring_<TAG>_offlinebundle.tar
    - datarobot_batch_scoring_<TAG>_executables.OSX.x86_64.tar
    - datarobot_batch_scoring_<TAG>_executables.Windows.x86_64.zip

  3. update release subject and description
  4. publish new release


Offline Bundle
--------------
This is a bundle which allows users to install datarobot_batch_scoring offline using the existing Python2.7 or 
Python3+ on the system. It includes all the dependencies and a script for bootstraps ``pip``.

We will release this on our Github release page. The archive contains:
  - helper_packages/ - packages like pip and virtualenv which can be used offline
  - required_packages/ - the batch_scoring script and its dependencies
  - OFFLINE_INSTALL_README.txt - install instructions 
  - get-pip.py - a script that allows us to bootstrap pip in user mode

This offline install method is suitable in situations where  Python 2.7 or Python 3+ are available. 
``sudo`` is not required.


PyInstaller single-file executable
----------------------------------

`See an overview of PyInstaller here <http://pyinstaller.readthedocs.io/en/stable/operating-mode.html>`_

PyInstaller bundles all the code and dependencies, including the Python interpreter, into a single 
directory or executable file. Right now we are creating three single-file
executables; ``batch_scoring_sse``, ``batch_scoring`` and ``batch_scoring_deployment_aware``.

We make a special image just for building this executable. 

**PyInstaller build instructions - Linux**

  **Dependencies:** Docker, docker-compose, Make

    1. build the image for PyInstaller with ``docker-compose build centos5pyinstaller``
    2. build just the pyinstaller executables with ``make pyinstaller_dockerized``. See the **Release** section of this readme for the official release process.
    3. test the build with ``test_pyinstaller_dockerized``

**PyInstaller build instructions - OSX / Other nixes**

  **Dependencies:** Make, Python>=3.5
  Note this build is performed on Travis CI and the artifacts are uploaded on PRs to s3://datarobot-batch-scoring-artifacts/ on both PRs and tags


**PyInstaller build instructions - Windows**

  Note this is done on Appveyor and the artifacts are uploaded on PRs to s3://datarobot-batch-scoring-artifacts/ on both PRs and tags

This is considered experimental because builds may not work on every platform
we need to support. For example, we need to be careful that linux apps are
forward compatible_, and our seperate builds_ for OSX and Windows have not been tested on many every versions of those OSs.


.. _compatible: http://pyinstaller.readthedocs.io/en/stable/usage.html#making-linux-apps-forward-compatible
.. _builds: http://pyinstaller.readthedocs.io/en/stable/usage.html#supporting-multiple-operating-systems
