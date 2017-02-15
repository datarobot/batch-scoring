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

Release 
-------

1. update ``__version__`` in ``datarobot_batch_scoring/__init__.py``
2. perform acceptance tests
3. tag release
4. push a tag to GitHub

  Travis bot runs automated tests and publish new version on PyPI when  
  tests are passed.
5. build the **PyInstaller** and **Offlinebundle** for Linux.
  1. build the image for PyInstaller with ``docker-compose build centos5pyinstaller``
  2. build both releases with ``make build_release_dockerized``
6. upload the builds producted by step 5. 
  1. find the release page for the version that was pushed at https://github.com/datarobot/batch-scoring/releases
  2. ``edit`` the release on github and attach the 4 files (2 zips, 2 tars) that were created in the **batch-scoring/dist** dir. ``save`` changes.


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
directory or executable file. Right now we are creating two single-file
executables; ``batch_scoring_sse`` and ``batch_scoring``.

We make a special image just for building this executable. 

**PyInstaller build instructions - Linux**

  **Dependencies:** Docker, docker-compose, Make

    1. build the image for PyInstaller with ``docker-compose build centos5pyinstaller``
    2. build just the pyinstaller executables with ``make pyinstaller_dockerized``. See the **Release** section of this readme for the official release process.

**PyInstaller build instructions - OSX / Other nixes**

  **Dependencies:** Make, Python>=3.4, zip

  TODO: formalize build/release/testing process

  The build works but we aren't ready to release. Dev builds can be made with ``make pyinstaller``

**PyInstaller build instructions - Windows**

  TODO: This should work but we don't have a build or release process for it yet.


The executables will be placed in 
``./dist/datarobot_batch_scoring_<version>_executables.<platform>.<arch>``.

This is considered experimental because it's untested, and may not work on every platform
we need to support. For example, we need to be careful that linux apps are
forward compatible_, and we would need separate builds_ for OSX and Windows.

.. _compatible: http://pyinstaller.readthedocs.io/en/stable/usage.html#making-linux-apps-forward-compatible
.. _builds: http://pyinstaller.readthedocs.io/en/stable/usage.html#supporting-multiple-operating-systems
